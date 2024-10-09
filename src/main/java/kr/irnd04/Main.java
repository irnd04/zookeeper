package kr.irnd04;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;

public class Main {

    public static int count = 0;

    public static void main(String[] args) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(10);

        for (int i = 0; i < 100; i++) {
            executor.execute(() -> {
               try {
                   globalLockSample();
               } catch (Exception e) {
                   e.printStackTrace();
               }
            });
        }

        executor.awaitTermination(2, TimeUnit.HOURS);
        executor.shutdown();

        System.out.println("count : " + count);
    }

    public static void globalLockSample() throws IOException, InterruptedException, KeeperException {
        // ZooKeeper 서버 주소를 인자로 전달하여 락 객체 생성
        GlobalLock lock = new GlobalLock("localhost:2181");

        try {
            // 락 획득
            lock.acquireLock();

            // 여기서 임계 영역(critical section)을 처리
            System.out.println("Doing some work while holding the lock...");
            count++;

            ThreadLocalRandom tlr = ThreadLocalRandom.current();
            TimeUnit.SECONDS.sleep(tlr.nextInt(10));

            // 락 해제
            lock.releaseLock();
        } finally {
            // ZooKeeper 연결 종료
            lock.close();
        }
    }
}