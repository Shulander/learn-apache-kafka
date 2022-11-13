package us.vicentini.kafka.util;

import lombok.extern.slf4j.Slf4j;

import java.util.Scanner;
import java.util.function.Consumer;

@Slf4j
    public class WaitKeyEvent implements Runnable {
        private final Consumer<String> doneEvent;


        public WaitKeyEvent(Consumer<String> tConsumer) {
            this.doneEvent = tConsumer;
        }


        @Override
        public void run() {
            Scanner keyboard = new Scanner(System.in);
            String entry = "asdf";
            while (!"".equalsIgnoreCase(entry)) {
                entry = keyboard.nextLine();
                log.info("new line: {}", entry);
            }
            doneEvent.accept(entry);

        }
    }
