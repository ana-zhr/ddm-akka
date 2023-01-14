package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.structures.Pair;
import de.ddm.structures.WorkMessage;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class DependencyWorker extends AbstractBehavior<DependencyWorker.Message> {
    public static final String DEFAULT_NAME = "dependencyWorker";
    ////////////////////
    // Actor Messages //
    ////////////////////
    private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

    private DependencyWorker(ActorContext<Message> context) {
        super(context);

        final ActorRef<Receptionist.Listing> listingResponseAdapter = context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
        context.getSystem().receptionist().tell(Receptionist.subscribe(DependencyMiner.dependencyMinerService, listingResponseAdapter));

        this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);
    }

    public static Behavior<Message> create() {
        return Behaviors.setup(DependencyWorker::new);
    }

    static ArrayList<Pair> inclusionDependencies(WorkMessage dependent, WorkMessage referenced) {
        ArrayList<Pair> ans = new ArrayList<>();
        for (int colInd = 0; colInd < referenced.columns(); colInd++) {
            Set<String> indVals = new HashSet<>();
            for (int i = 0; i < referenced.rows(); i++) {
                indVals.add(referenced.elem(i, colInd));
            }
            int len = indVals.size();
            for (int colDep = 0; colDep < dependent.columns(); colDep++) {
                boolean depends = true;
                for (int i = 0; i < dependent.rows(); i++) {
                    String cur = dependent.elem(i, colDep);
                    indVals.add(cur);
                    if (indVals.size() > len) {
                        indVals.remove(cur);
                        depends = false;
                    }
                }
                if (depends) {
                    ans.add(new Pair(dependent.getId(), referenced.getId(), dependent.col(colDep), referenced.col(colInd)));
                }
            }
        }
        return ans;
    }

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(ReceptionistListingMessage.class, this::handle)
                .onMessage(TaskMessage.class, this::handle)
                .onMessage(IdleMessage.class, this::handle)
                .build();
    }

    private Behavior<Message> handle(IdleMessage idleMessage) {
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        LargeMessageProxy.LargeMessage completionMessage = new DependencyMiner.CompletionMessage(this.getContext().getSelf(), new ArrayList<>());
        this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(completionMessage, idleMessage.getDependencyMinerLargeMessageProxy()));
        return this;
    }

    private Behavior<Message> handle(ReceptionistListingMessage message) {
        Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
        for (ActorRef<DependencyMiner.Message> dependencyMiner : dependencyMiners)
            dependencyMiner.tell(new DependencyMiner.RegistrationMessage(this.getContext().getSelf()));
        return this;
    }

    /////////////////
    // Actor State //
    /////////////////

    private Behavior<Message> handle(TaskMessage message) {
        this.getContext().getLog().info("Working!");
        List<Pair> deps = inclusionDependencies(message.getTask1(), message.getTask2());
        LargeMessageProxy.LargeMessage completionMessage = new DependencyMiner.CompletionMessage(this.getContext().getSelf(), deps);
        this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(completionMessage, message.getDependencyMinerLargeMessageProxy()));
        return this;
    }

    ////////////////////
    // Actor Behavior //
    ////////////////////

    public interface DependencyWorkerMessage extends AkkaSerializable, LargeMessageProxy.LargeMessage {
    }

    public interface Message extends AkkaSerializable {
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ReceptionistListingMessage implements Message {
        private static final long serialVersionUID = -5246338806092216222L;
        Receptionist.Listing listing;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class IdleMessage implements Message {
        private static final long serialVersionUID = -5246338806092216222L;
        ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TaskMessage implements Message {
        private static final long serialVersionUID = -4667745204456518160L;
        ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
        private WorkMessage task1;
        private WorkMessage task2;

        public TaskMessage(WorkMessage task1, WorkMessage task2, int id1, int id2) {
            this.task1 = task1;
            this.task2 = task2;

        }
    }
}
