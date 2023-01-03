package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.message.largeMessage.LargeMessage;
import de.ddm.actors.message.Message;
import de.ddm.actors.message.largeMessage.SendMessage;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.structures.InclusionDependency;
import de.ddm.structures.Task;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.*;

public class DependencyWorker extends AbstractBehavior<DependencyWorker.DependencyWorkerMessage> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface DependencyWorkerMessage extends AkkaSerializable, LargeMessage {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReceptionistListingDependencyWorkerMessage implements DependencyWorkerMessage {
		private static final long serialVersionUID = -5246338806092216222L;
		Receptionist.Listing listing;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class TaskDependencyWorkerMessage implements DependencyWorkerMessage {
		private static final long serialVersionUID = -4667745204456518160L;
		ActorRef<Message> dependencyMinerLargeMessageProxy;

		private Task task;
		private Map<String, Set<String>> distinctValuesA;
		private Map<String, Set<String>> distinctValuesB;

		private int getSetMemorySize(Set<String> set) {
			return set.stream().mapToInt(value -> value.length() * 2).sum();
		}

		public int getMemorySize() {
			return distinctValuesA.values().stream().mapToInt(this::getSetMemorySize).sum() + distinctValuesB.values().stream().mapToInt(this::getSetMemorySize).sum();
		}
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyWorker";

	public static Behavior<DependencyWorkerMessage> create() {
		return Behaviors.setup(DependencyWorker::new);
	}

	private DependencyWorker(ActorContext<DependencyWorkerMessage> context) {
		super(context);

		final ActorRef<Receptionist.Listing> listingResponseAdapter = context.messageAdapter(Receptionist.Listing.class, ReceptionistListingDependencyWorkerMessage::new);
		context.getSystem().receptionist().tell(Receptionist.subscribe(DependencyMiner.dependencyMinerService, listingResponseAdapter));

		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef<Message> largeMessageProxy;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<DependencyWorkerMessage> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReceptionistListingDependencyWorkerMessage.class, this::handle)
				.onMessage(TaskDependencyWorkerMessage.class, this::handle)
				.build();
	}

	private Behavior<DependencyWorkerMessage> handle(ReceptionistListingDependencyWorkerMessage message) {
		Set<ActorRef<LargeMessage>> dependencyMiners = message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
		for (ActorRef<LargeMessage> dependencyMiner : dependencyMiners)
			dependencyMiner.tell(new DependencyMiner.RegistrationLargeMessage(this.getContext().getSelf(), this.largeMessageProxy));
		return this;
	}


	private Behavior<DependencyWorkerMessage> handle(TaskDependencyWorkerMessage message) {
		this.getContext().getLog().info(
			message.task.getTableNameA(), message.task.getColumnNamesA().size(), message.distinctValuesA.values().stream().mapToInt(set -> set.size()).sum(),
			message.task.getTableNameB(), message.task.getColumnNamesB().size(), message.distinctValuesB.values().stream().mapToInt(set -> set.size()).sum(),
			message.getMemorySize());

		List<InclusionDependency> inclusionDeps = new ArrayList<>();
		message.distinctValuesA.forEach((columnA, setA) -> {
			message.distinctValuesB.forEach((columnB, setB) -> {
			        if (columnA == columnB) {
			            return;
				}
				for (InclusionDependency dep : inclusionDeps) {
				    if (dep.getDependentColumn() == columnA
				    && dep.getReferencedColumn() == columnB){
				       return;
				    }
				}
				
				int cardinalityA = setA.size();
				int cardinalityB = setB.size();


				if (cardinalityA <= cardinalityB && setB.containsAll(setA)) {
					inclusionDeps.add(new InclusionDependency(message.task.getTableNameA(), message.task.getTableNameB(), columnA, columnB));
				}
				if (cardinalityB <= cardinalityA && setA.containsAll(setB)) {
					inclusionDeps.add(new InclusionDependency(message.task.getTableNameB(), message.task.getTableNameA(), columnB, columnA));
				}
			});
		});

		this.getContext().getLog().info(
			"Found {} INDs for table {} and table {}: {}",
			inclusionDeps.size(), message.task.getTableNameA(), message.task.getTableNameB(), inclusionDeps);

		LargeMessage completionMessage = new DependencyMiner.CompletionLargeMessage(this.getContext().getSelf(), inclusionDeps);
		this.largeMessageProxy.tell(new SendMessage(completionMessage, message.getDependencyMinerLargeMessageProxy()));

		return this;
	}
}
