package de.ddm.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.DispatcherSelector;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.ddm.actors.patterns.Reaper;
import de.ddm.actors.profiling.DependencyMiner;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.structures.WorkMessage;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class Master extends AbstractBehavior<Master.Message> {

    ////////////////////
    // Actor Messages //
    ////////////////////

    public static final String DEFAULT_NAME = "master";
    private final Queue<WorkMessage> unassignedTasks = new LinkedList<>();
    private final Queue<ActorRef<Worker.Message>> idleWorkers = new LinkedList<>();

    ////////////////////////
    // Actor Construction //
    ////////////////////////
    private final Map<ActorRef<Worker.Message>, WorkMessage> busyWorkers = new HashMap<>();
    private final ActorRef<DependencyMiner.Message> dependencyMiner;

    private Master(ActorContext<Message> context) {
        super(context);
        Reaper.watchWithDefaultReaper(this.getContext().getSelf());

        this.dependencyMiner = context.spawn(DependencyMiner.create(), DependencyMiner.DEFAULT_NAME, DispatcherSelector.fromConfig("akka.master-pinned-dispatcher"));
    }

    public static Behavior<Message> create() {
        return Behaviors.setup(Master::new);
    }

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(StartMessage.class, this::handle)
                .onMessage(ShutdownMessage.class, this::handle)
                .build();
    }

    private Behavior<Message> handle(StartMessage message) {
        this.dependencyMiner.tell(new DependencyMiner.StartMessage());
        return this;
    }
    /////////////////
    // Actor State //
    /////////////////

    private Behavior<Message> handle(ShutdownMessage message) {
        // If we expect the system to still be active when the a ShutdownMessage is issued,
        // we should propagate this ShutdownMessage to all active child actors so that they
        // can end their protocols in a clean way. Simply stopping this actor also stops all
        // child actors, but in a hard way!
        return Behaviors.stopped();
    }

    ////////////////////
    // Actor Behavior //
    ////////////////////

    public interface Message extends AkkaSerializable {
    }

    @NoArgsConstructor
    public static class StartMessage implements Message {
        private static final long serialVersionUID = -1963913294517850454L;
    }

    @NoArgsConstructor
    public static class ShutdownMessage implements Message {
        private static final long serialVersionUID = 7516129288777469221L;
    }
}