package de.ddm.actors.message.largeMessage;

import de.ddm.actors.message.Message;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public class BytesMessage implements Message {
		private static final long serialVersionUID = -8435193720156121630L;
		private byte[] bytes;
		private int senderTransmissionKey;
		private int receiverTransmissionKey;
	}