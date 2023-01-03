package de.ddm.actors.message.largeMessage;

import de.ddm.actors.message.Message;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public  class ConnectAckMessage implements Message {
		private static final long serialVersionUID = 6497424731575554980L;
		private int senderTransmissionKey;
		private int receiverTransmissionKey;
	}