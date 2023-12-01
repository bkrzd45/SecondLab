import java.util.Map;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


class Event {
    private String message;

    public Event(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}

interface EventHandler {
    void handleEvent(Event event);
}


class EventBus {
    private Map<String, CopyOnWriteArrayList<EventHandler>> topicSubscribers = new ConcurrentHashMap<>();
    private Map<String, ExecutorService> topicExecutors = new ConcurrentHashMap<>();

    private final Lock lock = new ReentrantLock();

    public void subscribe(String topic, EventHandler handler) {
        topicSubscribers.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>()).add(handler);
    }

    public void unsubscribe(String topic, EventHandler handler) {
        topicSubscribers.computeIfPresent(topic, (k, v) -> {
            v.remove(handler);
            return v.isEmpty() ? null : v;
        });
    }

    public void publish(String topic, Event event) {
        lock.lock();
        try {
            CopyOnWriteArrayList<EventHandler> subscribers = topicSubscribers.getOrDefault(topic, new CopyOnWriteArrayList<>());
            for (EventHandler handler : subscribers) {
                getExecutorForTopic(topic).execute(() -> handler.handleEvent(event));
            }
        } finally {
            lock.unlock();
        }
    }

    private ExecutorService getExecutorForTopic(String topic) {
        return topicExecutors.computeIfAbsent(topic, k -> Executors.newFixedThreadPool(5));
    }

    public void shutdown() {
        topicExecutors.values().forEach(ExecutorService::shutdown);
    }
}

class EventBusWithLocks {
    private final Map<String, CopyOnWriteArrayList<EventHandler>> topicSubscribers = new ConcurrentHashMap<>();
    private final Map<String, ExecutorService> topicExecutors = new ConcurrentHashMap<>();
    private final Map<String, Lock> topicLocks = new HashMap<>();

    public void subscribe(String topic, EventHandler handler) {
        Lock lock = getLockForTopic(topic);
        lock.lock();
        try {
            List<EventHandler> subscribers = topicSubscribers.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>());
            subscribers.add(handler);
        } finally {
            lock.unlock();
        }
    }

    public void unsubscribe(String topic, EventHandler handler) {
        Lock lock = getLockForTopic(topic);
        lock.lock();
        try {
            List<EventHandler> subscribers = topicSubscribers.get(topic);
            if (subscribers != null) {
                subscribers.remove(handler);
                if (subscribers.isEmpty()) {
                    topicSubscribers.remove(topic);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public void publish(String topic, Event event) {
        Lock lock = getLockForTopic(topic);
        lock.lock();
        try {
            List<EventHandler> subscribers = topicSubscribers.get(topic);
            if (subscribers != null) {
                for (EventHandler handler : subscribers) {
                    getExecutorForTopic(topic).execute(() -> handler.handleEvent(event));
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private Lock getLockForTopic(String topic) {
        return topicLocks.computeIfAbsent(topic, k -> new ReentrantLock());
    }

    private ExecutorService getExecutorForTopic(String topic) {
        return topicExecutors.computeIfAbsent(topic, k -> Executors.newFixedThreadPool(5));
    }

    public void shutdown() {
        topicExecutors.values().forEach(ExecutorService::shutdown);
    }
}


public class Main {
    public static void main(String[] args) {
        System.out.println("В лабораторной работе реализовано 2 варианта шины событий, \nсейчас на тесте шина с уменьшенным количеством блокировок\n");
        EventBus eventBus = new EventBus();

        eventBus.subscribe("example", event -> {
            System.out.println("Received event: {" + event.getMessage() + "} from thread named: " + Thread.currentThread().getName());
        });

        for (int i = 1; i <= 10; i++) {
            Event event = new Event("My message " + i);
            eventBus.publish("example", event);
        }

        eventBus.shutdown();
    }
}
