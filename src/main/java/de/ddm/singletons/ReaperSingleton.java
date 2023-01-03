package de.ddm.singletons;

import akka.actor.typed.ActorRef;
import de.ddm.actors.message.Message;

public class ReaperSingleton {

	private static ActorRef<Message> singleton;

	public static ActorRef<Message> get() {
		return singleton;
	}

	public static void set(ActorRef<Message> instance) {
		singleton = instance;
	}
}
