package de.ddm.actors.message.largeMessage;

import akka.actor.typed.ActorRef;
import de.ddm.actors.message.Message;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public class SendMessage implements Message {
		private static final long serialVersionUID = -1203695340601241430L;
		private LargeMessage message;
		private ActorRef<Message> receiverProxy;
	}