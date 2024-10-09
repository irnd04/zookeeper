package kr.irnd04;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

// https://zookeeper.apache.org/doc/r3.9.2/recipes.html
public class GlobalLock {

    private static final String LOCK_ROOT_PATH = "/locks";  // 락의 루트 노드 경로
    private static final String LOCK_NODE_PREFIX = LOCK_ROOT_PATH + "/lock_";  // 락 노드의 접두사
    private String lockPath;  // 생성된 임시 노드의 경로
    private ZooKeeper zk;  // ZooKeeper 클라이언트 객체

    // ZooKeeper 연결 설정 및 락 노드 생성
    public GlobalLock(String zookeeperAddress) throws IOException, KeeperException, InterruptedException {
        zk = new ZooKeeper(zookeeperAddress, 3000, event -> {
            // 이벤트 처리 로직 (필요시 구현)
        });

        // 루트 락 경로가 없다면 생성
        Stat stat = zk.exists(LOCK_ROOT_PATH, false);
        if (stat == null) {
            zk.create(LOCK_ROOT_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    // 분산 락을 획득하는 메서드
    public void acquireLock() throws KeeperException, InterruptedException {
        // 임시 순차 노드를 생성
        lockPath = zk.create(LOCK_NODE_PREFIX, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("Lock path created: " + lockPath);

        // 락을 획득할 수 있는지 확인
        tryLock();
    }

    // 락을 시도하는 메서드
    private void tryLock() throws KeeperException, InterruptedException {
        final Object lock = new Object();

        while (true) {
            // 루트 경로 아래에 있는 모든 자식 노드(락 노드) 목록을 가져옴
            List<String> children = zk.getChildren(LOCK_ROOT_PATH, false);

            // 순차적으로 생성된 노드 이름으로 정렬
            Collections.sort(children);

            String myNodeName = lockPath.substring(LOCK_ROOT_PATH.length() + 1);
            int myNodeIndex = children.indexOf(myNodeName);

            // 내가 생성한 노드가 가장 작은 순번이면 락을 획득한 것임
            if (myNodeIndex == 0) {
                System.out.println("Lock acquired: " + myNodeName);
                return;
            }

            // 가장 작은 노드가 아니면 바로 앞 노드의 이벤트를 대기함
            String watchNode = children.get(myNodeIndex - 1);

            // 바로 앞 노드를 Watcher로 설정하여 해당 노드가 삭제될 때까지 대기
            final String watchNodePath = LOCK_ROOT_PATH + "/" + watchNode;

            // Watcher 등록 및 이벤트 처리
            Watcher nodeWatcher = event -> {
                if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
                    synchronized (lock) {
                        lock.notifyAll(); // 노드가 삭제되면 알림
                    }
                }
            };

            Stat stat = zk.exists(watchNodePath, nodeWatcher); // 노드 존재 여부 확인 및 Watch 등록

            // 노드가 존재하지 않으면 처음부터 다시 시작
            if (stat == null) {
                continue;
            }

            // 노드가 존재하면 3초 동안 대기 (이후 다시 Watch를 등록하여 감시)
            System.out.println("Waiting for node deletion: " + watchNodePath);

            synchronized (lock) {
                lock.wait(3000L);
            }

            // 시간이 지나도 이벤트가 발생하지 않으면 처음부터 다시 시작
        }

    }

    // 락을 해제하는 메서드
    public void releaseLock() throws KeeperException, InterruptedException {
        // 락 노드를 삭제하여 락 해제
        if (lockPath != null) {
            zk.delete(lockPath, -1);
            System.out.println("Lock released: " + lockPath);
            lockPath = null;
        }
    }

    // ZooKeeper 연결 종료
    public void close() throws InterruptedException {
        if (zk != null) {
            zk.close();
        }
    }
}