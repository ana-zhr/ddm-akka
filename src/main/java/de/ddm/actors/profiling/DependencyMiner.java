package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.actors.message.largeMessage.LargeMessage;
import de.ddm.actors.message.Message;
import de.ddm.actors.message.largeMessage.SendMessage;
import de.ddm.actors.message.largeMessage.StartLargeMessage;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import de.ddm.structures.Task;
import de.ddm.structures.TaskGenerator;
import de.ddm.structures.LocalDataStorage;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.*;

public class DependencyMiner extends AbstractBehavior<LargeMessage> {

	////////////////////
	// Actor Messages //
	////////////////////

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class HeaderLargeMessage implements LargeMessage {
		private static final long serialVersionUID = -5322425954432915838L;
		private int id;
		private String[] header;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class BatchLargeMessage implements LargeMessage {
		private static final long serialVersionUID = 4591192372652568030L;
		private int id;
		private List<String[]> rows;

		public boolean finishedReading(){
			return rows.isEmpty();
		}
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RegistrationLargeMessage implements LargeMessage {
		private static final long serialVersionUID = -4025238529984914107L;
		private ActorRef<DependencyWorker.DependencyWorkerMessage> dependencyWorker;
		private ActorRef<Message> dependencyWorkerLargeMessageProxy;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class CompletionLargeMessage implements LargeMessage {
		private static final long serialVersionUID = -7642425159675583598L;
		private ActorRef<DependencyWorker.DependencyWorkerMessage> dependencyWorker;

		private List<InclusionDependency> inclusionDependencies;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyMiner";

	public static final ServiceKey<LargeMessage> dependencyMinerService = ServiceKey.create(LargeMessage.class, DEFAULT_NAME + "Service");

	public static Behavior<LargeMessage> create() {
		return Behaviors.setup(DependencyMiner::new);
	}

	private DependencyMiner(ActorContext<LargeMessage> context) {
		super(context);
		this.discoverNaryDependencies = SystemConfigurationSingleton.get().isHardMode();

		this.memUsage = 0;
		this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
		this.finishedReading = new boolean[this.inputFiles.length];

		this.inputReaders = new ArrayList<>(inputFiles.length);
		for (int id = 0; id < this.inputFiles.length; id++)
			this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
		this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);

		context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
	}

	/////////////////
	// Actor State //
	/////////////////

	private long startTime;

	private int memUsage;
	private final boolean discoverNaryDependencies;
	private final File[] inputFiles;
	private final LocalDataStorage dataStorage = new LocalDataStorage();
	private final boolean[] finishedReading;

	private final List<ActorRef<Message>> inputReaders;
	private final ActorRef<Message> resultCollector;
	private final ActorRef<Message> largeMessageProxy;


	private final List<ActorRef<DependencyWorker.DependencyWorkerMessage>> dependencyWorkers = new ArrayList<>();
	private final List<ActorRef<Message>> dependencyWorkerLargeProxies = new ArrayList<>();

	private final Queue<Task> unassignedTasks = new ArrayDeque<>();

	private final Map<ActorRef<DependencyWorker.DependencyWorkerMessage>, Task> busyWorkers = new HashMap<>();

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<LargeMessage> createReceive() {
		return newReceiveBuilder()
				.onMessage(StartLargeMessage.class, this::handle)
				.onMessage(HeaderLargeMessage.class, this::handle)
				.onMessage(BatchLargeMessage.class, this::handle)
				.onMessage(RegistrationLargeMessage.class, this::handle)
				.onMessage(CompletionLargeMessage.class, this::handle)
				.onSignal(Terminated.class, this::handle)
				.build();
	}

	private Behavior<LargeMessage> handle(StartLargeMessage message) {

		for (ActorRef<Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));

		this.startTime = System.currentTimeMillis();

		return this;
	}

	private Behavior<LargeMessage> handle(HeaderLargeMessage message) {
				String tableName = this.inputFiles[message.id].getName();
		this.dataStorage.addTable(tableName, Arrays.asList(message.header));

		this.inputReaders.get(message.id).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));

		return this;
	}


	private void delegateTasks(){

		for (int workerIdx = 0; workerIdx < this.dependencyWorkers.size(); ++workerIdx){
			if (this.unassignedTasks.isEmpty()) {
				break;
			}

		    ActorRef<DependencyWorker.DependencyWorkerMessage> worker = this.dependencyWorkers.get(workerIdx);
		    ActorRef<Message> workerProxy = this.dependencyWorkerLargeProxies.get(workerIdx);

			if (this.busyWorkers.containsKey(worker)) {
				continue;
			}


			assignTask(worker, workerProxy);
		}
		this.getContext().getLog().info("After task delegation: {} unassigned tasks", this.unassignedTasks.size());
	}

	private void assignTask(ActorRef<DependencyWorker.DependencyWorkerMessage> worker, ActorRef<Message> workerProxy) {
		Task task = this.unassignedTasks.remove();

		Map<String, Set<String>> distinctValuesA = new HashMap<>();
		Map<String, Set<String>> distinctValuesB = new HashMap<>();
		for (String colName: task.getColumnNamesA()) {
			distinctValuesA.put(colName, this.dataStorage.getColumn(task.getTableNameA(), colName).getDistinctValues());
		}
		for (String colName: task.getColumnNamesB()) {
			distinctValuesB.put(colName, this.dataStorage.getColumn(task.getTableNameB(), colName).getDistinctValues());
		}

		DependencyWorker.TaskDependencyWorkerMessage taskMessage = new DependencyWorker.TaskDependencyWorkerMessage(
			this.largeMessageProxy,
			task, distinctValuesA, distinctValuesB);

		this.largeMessageProxy.tell(new SendMessage(taskMessage, workerProxy));
		this.busyWorkers.put(worker, task);
	}

	private Behavior<LargeMessage> handle(BatchLargeMessage message) {
		String tableName = this.inputFiles[message.id].getName();
		if (message.finishedReading()) {
			this.finishedReading[message.id] = true;


			for (int id = 0; id < this.inputFiles.length; ++id) {

				checkAndOptionallyCreateTasks(tableName, id);
			}

			delegateTasks();
		} else {
			this.dataStorage.addRows(tableName, message.rows.stream().map(row -> Arrays.asList(row)));


			this.inputReaders.get(message.id).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
		}

		return this;
	}

	private void checkAndOptionallyCreateTasks(String tableName, int id) {
		if (this.finishedReading[id]) {
			String otherTableName = this.inputFiles[id].getName();
			List<Task> tasks = TaskGenerator.run(
				dataStorage,
				60 * 1024 * 1024,
					tableName,
				otherTableName);
			tasks.forEach(task -> this.getContext().getLog().info("Task: {}", task));

			this.unassignedTasks.addAll(tasks);
		}
	}

	private Behavior<LargeMessage> handle(RegistrationLargeMessage message) {

		ActorRef<DependencyWorker.DependencyWorkerMessage> dependencyWorker = message.getDependencyWorker();
		if (!this.dependencyWorkers.contains(dependencyWorker)) {
			this.dependencyWorkers.add(dependencyWorker);
			this.getContext().watch(dependencyWorker);
			this.dependencyWorkerLargeProxies.add(message.getDependencyWorkerLargeMessageProxy());

			delegateTasks();
		}
		return this;
	}

	private Behavior<LargeMessage> handle(CompletionLargeMessage message) {
		ActorRef<DependencyWorker.DependencyWorkerMessage> dependencyWorker = message.getDependencyWorker();
		Task task = this.busyWorkers.get(message.getDependencyWorker());
		List<InclusionDependency> inds = message.getInclusionDependencies();
		if (!inds.isEmpty()) {
			this.resultCollector.tell(new ResultCollector.ResultMessage(inds));
		}


		this.busyWorkers.remove(dependencyWorker);


		delegateTasks();

		// system finish
		boolean finishedAll = true;
		for (boolean b: this.finishedReading){
			if (!b) {
				finishedAll = false;
				break;
			}
		}
		if (finishedAll && this.unassignedTasks.isEmpty() && this.busyWorkers.isEmpty())
			this.end();

		return this;
	}

	private void end() {
		this.resultCollector.tell(new ResultCollector.FinalizeMessage());
		long discoveryTime = System.currentTimeMillis() - this.startTime;
		this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);
	}

	private Behavior<LargeMessage> handle(Terminated signal) {
		ActorRef<DependencyWorker.DependencyWorkerMessage> dependencyWorker = signal.getRef().unsafeUpcast();
		this.dependencyWorkers.remove(dependencyWorker);
		return this;
	}
}
