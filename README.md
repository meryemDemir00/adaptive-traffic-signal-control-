# Gerçek Zamanlı Büyük Veri Tabanlı Adaptif Trafik Sinyal Kontrol Sistemi

Yazılım Geliştirme – I, 2. Proje | Kocaeli Üniversitesi

## Proje Özeti
SUMO ile simüle edilen bir yol ağındaki trafik verisi Kafka ve Spark Structured Streaming ile gerçek zamanlı işlenir; eşik tabanlı bir karar servisi sinyal planlarını uyarlar. Sabit zamanlı kontrol ile adaptif kontrol karşılaştırılır.

## Mimari
SUMO → Python (TraCI) → Kafka → Spark Streaming → ClickHouse → Grafana

Karar servisi: metrics topic'ini okur, control topic'ine komut yazar.

## Ekip
- Meryem
- Hatice

## Gereksinimler
- Docker ve Docker Compose
- (Yerel geliştirme için) SUMO, Python 3.x

## Çalıştırma
> Doldurulacak.

## Klasör Yapısı
> Doldurulacak.

## Kaynaklar ve Üçüncü Taraf Kütüphaneler
- SUMO, OpenStreetMap, Apache Kafka, Apache Spark, ClickHouse, Grafana
