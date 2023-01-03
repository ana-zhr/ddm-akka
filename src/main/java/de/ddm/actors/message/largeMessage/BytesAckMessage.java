package de.ddm.actors.message.largeMessage;

import de.ddm.actors.message.Message;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public class BytesAckMessage implements Message {
		private static final long serialVersionUID = 5992096322167014051L;
		private int senderTransmissionKey;
		private int receiverTransmissionKey;
	}