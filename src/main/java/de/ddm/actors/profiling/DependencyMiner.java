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
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import de.ddm.structures.Pair;
import de.ddm.structures.WorkMessage;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.*;

public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {

    ////////////////////
    // Actor Messages //
    ////////////////////

    public static final String DEFAULT_NAME = "dependencyMiner";
    public static final ServiceKey<DependencyMiner.Message> dependencyMinerService = ServiceKey.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service");
    private final Map<ActorRef<DependencyWorker.Message>, WorkMessage> busyWorkers = new HashMap<ActorRef<DependencyWorker.Message>, WorkMessage>();
    private final Queue<WorkMessage> unassignedWork = new LinkedList<>();
    private final Queue<ActorRef> idleWorkers = new LinkedList<>();
    private final File[] inputFiles;
    private final String[][] headerLines;
    private final boolean discoverNaryDependencies;
    private final List<ActorRef<InputReader.Message>> inputReaders;
    private final ActorRef<ResultCollector.Message> resultCollector;
    private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;
    private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers;
    private List<BatchMessage> batches;
    private int firstFile = 0;
    private int secondFile = 0;

    ////////////////////////
    // Actor Construction //
    ////////////////////////
    private int checkBound = 0;
    private long startTime;

    private DependencyMiner(ActorContext<Message> context) {
        super(context);
        this.batches = new ArrayList<>();
        this.discoverNaryDependencies = SystemConfigurationSingleton.get().isHardMode();
        this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
        this.headerLines = new String[this.inputFiles.length][];

        this.inputReaders = new ArrayList<>(inputFiles.length);
        for (int id = 0; id < this.inputFiles.length; id++)
            this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
        this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
        this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);

        this.dependencyWorkers = new ArrayList<>();

        context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
    }

    public static Behavior<Message> create() {
        return Behaviors.setup(DependencyMiner::new);
    }

    /////////////////
    // Actor State //
    /////////////////

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(StartMessage.class, this::handle)
                .onMessage(BatchMessage.class, this::handle)
                .onMessage(HeaderMessage.class, this::handle)
                .onMessage(RegistrationMessage.class, this::handle)
                .onMessage(CompletionMessage.class, this::handle)
                .onSignal(Terminated.class, this::handle)
                .build();
    }

    private Behavior<Message> handle(StartMessage message) {
        for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
            inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));
        for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
            inputReader.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
        this.startTime = System.currentTimeMillis();
        return this;
    }

    private Behavior<Message> handle(HeaderMessage message) {
        this.headerLines[message.getId()] = message.getHeader();
        return this;
    }

    private Behavior<Message> handle(BatchMessage message) {
        batches.add(message);

        return this;
    }

    private Behavior<Message> handle(RegistrationMessage message) {
        ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
        if (this.busyWorkers.containsKey(dependencyWorker) || this.idleWorkers.contains(dependencyWorker))
            return this;
        this.getContext().watch(dependencyWorker);
        sendMessage(dependencyWorker);
        return this;
    }

    private void sendMessage(ActorRef<DependencyWorker.Message> dependencyWorker) {
        if (batches.size() != inputReaders.size()) {
            checkBound++;
            dependencyWorker.tell(new DependencyWorker.IdleMessage(this.largeMessageProxy));
        }
        if (firstFile >= inputReaders.size() && checkBound != 0) {
            return;
        } else if (isFinished()) {
            end();
        } else {
            BatchMessage batch1 = batches.get(firstFile);
            BatchMessage batch2 = batches.get(secondFile);
            secondFile++;
            if (secondFile >= batches.size()) {
                firstFile++;
                secondFile = firstFile;
            }
            checkBound++;
            dependencyWorker.tell(new DependencyWorker.TaskMessage(this.largeMessageProxy, new WorkMessage(batch1.getBatch(), batch1.getId(), headerLines[batch1.getId()]), new WorkMessage(batch2.getBatch(), batch2.getId(), headerLines[batch2.getId()])));
        }

    }

    ////////////////////
    // Actor Behavior //
    ////////////////////

    private boolean isFinished() {
        return firstFile >= inputReaders.size();
    }

    private Behavior<Message> handle(CompletionMessage message) {
        ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
        checkBound--;
        if (null != message.getDep()) {
            if (message.getDep().size() != 0 && this.headerLines[0] != null) {
                for (Pair dep : message.getDep()) {
                    int dependent = dep.getTableDependentId();
                    int referenced = dep.getTableIndId();
                    File dependentFile = this.inputFiles[dependent];
                    File referencedFile = this.inputFiles[referenced];
                    String[] dependentAttributes = {dep.getDependent()};
                    String[] referencedAttributes = {dep.getReferencedId()};

                    InclusionDependency ind = new InclusionDependency(dependentFile, dependentAttributes, referencedFile, referencedAttributes);
                    List<InclusionDependency> inds = new ArrayList<>(1);
                    inds.add(ind);

                    this.resultCollector.tell(new ResultCollector.ResultMessage(inds));
                }
            }

        }

        this.getContext().getLog().info("Completed work for task {}");

        sendMessage(dependencyWorker);

        if (isFinished())
            end();

        return this;
    }

    private void end() {
        this.resultCollector.tell(new ResultCollector.FinalizeMessage());
        long discoveryTime = System.currentTimeMillis() - this.startTime;
        this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);
    }

    private Behavior<Message> handle(Terminated signal) {
        ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
        this.dependencyWorkers.remove(dependencyWorker);
        return this;
    }

    public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
    }

    @NoArgsConstructor
    public static class StartMessage implements Message {
        private static final long serialVersionUID = -1963913294517850454L;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class HeaderMessage implements Message {
        private static final long serialVersionUID = -5322425954432915838L;
        int id;
        String[] header;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BatchMessage implements Message {
        private static final long serialVersionUID = 4591192372652568030L;
        int id;
        List<String[]> batch;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RegistrationMessage implements Message {
        private static final long serialVersionUID = -4025238529984914107L;
        ActorRef<DependencyWorker.Message> dependencyWorker;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CompletionMessage implements Message {
        private static final long serialVersionUID = -7642425159675583598L;
        ActorRef<DependencyWorker.Message> dependencyWorker;
        List<Pair> dep;


    }
}