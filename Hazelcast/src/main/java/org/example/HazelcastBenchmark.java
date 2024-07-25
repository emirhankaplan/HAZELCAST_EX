package org.example;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class HazelcastBenchmark {
    private static final int NUM_RECORDS_1 = 20000;
    private static final int NUM_RECORDS_2 = 100000;

    public static void main(String[] args) {
        // Hazelcast istemci konfigürasyonunu oluşturun
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress("localhost"); // Hazelcast sunucu adresi

        // Hazelcast kümesine bağlanın
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        IMap<Integer, Integer> map = client.getMap("benchmark");

        // Benchmark testlerini çalıştırın
        benchmarkHazelcast(map, 20000);
        benchmarkHazelcast(map, 100000);

        // İstemciyi kapatmadan bekleme
        System.out.println("Press Ctrl+C to shut down the client.");
        try {
            Thread.sleep(Long.MAX_VALUE); // İstemciyi kapatmadan bekle
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // İstemciyi kapatın
        client.shutdown();
    }

    private static void benchmarkHazelcast(IMap<Integer, Integer> map, int numRecords) {
        Random random = new Random();
        long startTime, endTime;

        // Rastgele sayıları ekleyin
        startTime = System.nanoTime();
        for (int i = 0; i < numRecords; i++) {
            map.put(i, random.nextInt());
        }
        endTime = System.nanoTime();
        System.out.println(numRecords + " put: " + TimeUnit.NANOSECONDS.toMillis(endTime - startTime) + " ms");

        // Rastgele sayıları alın
        startTime = System.nanoTime();
        for (int i = 0; i < numRecords; i++) {
            map.get(random.nextInt(numRecords));
        }
        endTime = System.nanoTime();
        System.out.println(numRecords + " get: " + TimeUnit.NANOSECONDS.toMillis(endTime - startTime) + " ms");
    }
}
