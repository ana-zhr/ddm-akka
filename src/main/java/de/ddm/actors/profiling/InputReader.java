package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import de.ddm.actors.message.largeMessage.LargeMessage;
import de.ddm.actors.message.Message;
import de.ddm.singletons.DomainConfigurationSingleton;
import de.ddm.singletons.InputConfigurationSingleton;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InputReader extends AbstractBehavior<Message> {

	////////////////////
	// Actor Messages //
	////////////////////



	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReadHeaderMessage implements Message {
		private static final long serialVersionUID = 1729062814525657711L;
		private ActorRef<LargeMessage> replyTo;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReadBatchMessage implements Message {
		private static final long serialVersionUID = -7915854043207237318L;
		private ActorRef<LargeMessage> replyTo;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "inputReader";

	public static Behavior<Message> create(final int id, final File inputFile) {
		return Behaviors.setup(context -> new InputReader(context, id, inputFile));
	}

	private InputReader(ActorContext<Message> context, final int id, final File inputFile) throws IOException, CsvValidationException {
		super(context);
		this.id = id;
		this.reader = InputConfigurationSingleton.get().createCSVReader(inputFile);
		this.header = reader.readNext(); // read header
		assert this.header != null : "Failed to read header of input file " + inputFile.getName();
	}

	/////////////////
	// Actor State //
	/////////////////

	private final int id;
	private final int batchSize = DomainConfigurationSingleton.get().getInputReaderBatchSize();
	private final CSVReader reader;
	private final String[] header;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReadHeaderMessage.class, this::handle)
				.onMessage(ReadBatchMessage.class, this::handle)
				.onSignal(PostStop.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(ReadHeaderMessage message) {
		message.getReplyTo().tell(new DependencyMiner.HeaderLargeMessage(this.id, this.header));
		return this;
	}

	private Behavior<Message> handle(ReadBatchMessage message) throws IOException, CsvValidationException {
		List<String[]> batch = new ArrayList<>(this.batchSize);
		for (int i = 0; i < this.batchSize; i++) {
			String[] line = this.reader.readNext();
			if (line == null)
				break;
			batch.add(line);
		}

		message.getReplyTo().tell(new DependencyMiner.BatchLargeMessage(this.id, batch));
		return this;
	}

	private Behavior<Message> handle(PostStop signal) throws IOException {
		this.reader.close();
		return this;
	}
}