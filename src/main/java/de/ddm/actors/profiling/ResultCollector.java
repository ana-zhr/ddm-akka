package de.ddm.actors.profiling;

import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.ddm.actors.Guardian;
import de.ddm.actors.message.Message;
import de.ddm.singletons.DomainConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class ResultCollector extends AbstractBehavior<Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ResultMessage implements Message {
		private static final long serialVersionUID = -7070569202900845736L;
		private List<InclusionDependency> inclusionDependencies;
	}

	@NoArgsConstructor
	public static class FinalizeMessage implements Message {
		private static final long serialVersionUID = -6603856949941810321L;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "resultCollector";

	public static Behavior<Message> create() {
		return Behaviors.setup(ResultCollector::new);
	}

	private ResultCollector(ActorContext<Message> context) throws IOException {
		super(context);

		String outputFileName = DomainConfigurationSingleton.get().getResultCollectorOutputFileName();

		File txt_file = new File(outputFileName + ".txt");
		if (txt_file.exists() && !txt_file.delete())
			throw new IOException("Could not delete existing result file: " + txt_file.getName());
		if (!txt_file.createNewFile())
			throw new IOException("Could not create result file: " + txt_file.getName());

		this.txt_writer = new BufferedWriter(new FileWriter(txt_file));
	}

	/////////////////
	// Actor State //
	/////////////////

	private final BufferedWriter txt_writer;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ResultMessage.class, this::handle)
				.onMessage(FinalizeMessage.class, this::handle)
				.onSignal(PostStop.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(ResultMessage message) throws IOException {
		this.getContext().getLog().info("Received {} INDs!", message.getInclusionDependencies().size());

		for (InclusionDependency ind : message.getInclusionDependencies()) {
			this.txt_writer.write(ind.toString());
			this.txt_writer.newLine();
		}
		this.txt_writer.flush();

		return this;
	}

	private Behavior<Message> handle(FinalizeMessage message) throws IOException {
		this.getContext().getLog().info("Received FinalizeMessage!");

		this.txt_writer.flush();

		this.getContext().getSystem().unsafeUpcast().tell(new Guardian.ShutdownMessage());

		return this;
	}

	private Behavior<Message> handle(PostStop signal) throws IOException {
		this.txt_writer.close();

		return this;
	}
}
