package de.ddm.actors.message.largeMessage;

import akka.actor.typed.ActorRef;
import de.ddm.actors.message.Message;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public class ConnectMessage implements Message {
		private static final long serialVersionUID = -2368932735326858722L;
		private int senderTransmissionKey;
		private ActorRef<Message> senderProxy;
		private int largeMessageSize;
		private int serializerId;
		private String manifest;
	}