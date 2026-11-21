# Kafka Mesaj Sözleşmesi (TASLAK)

Bu belge, servisler arasında Kafka üzerinden akan mesajların alanlarını, tiplerini ve birimlerini tanımlar. Producer (Kişi A), Spark job'u ve controller (Kişi B) bu sözleşmeye göre yazılır. Değişiklik gerekirse önce bu dosya güncellenir, sonra kod.

> Durum: \*\*v1, ekip kararı verildi.\*\* Kararlar en altta "Kararlar" bölümünde. `tls\_id` değerleri harita hazır olunca eklenecek.

## Genel kurallar

|Konu|Karar|
|-|-|
|Mesaj biçimi|JSON, UTF-8, tek satır|
|Zaman damgaları|`\*\_ms` ile biten alanlar epoch milisaniye (tamsayı)|
|Simülasyon zamanı|`sim\_time` saniye (ondalıklı olabilir)|
|Hız|m/s|
|Bekleme süresi|saniye|
|Doluluk (occupancy)|yüzde, 0-100 (SUMO'nun döndürdüğü değer)|
|Kuyruk|durmakta olan araç sayısı (SUMO `getLastStepHaltingNumber`)|
|Kafka anahtarı (key)|`run\_id:tls\_id`|
|Topic adları|`.env.example` içindeki değerler: `traffic.raw`, `traffic.metrics`, `traffic.control`|

`event\_time\_ms = run\_start\_ms + sim\_time \* 1000`. Spark pencereleri bu alanla çalışır, böylece simülasyon hızı sonucu etkilemez ve farklı koşular birbirine karışmaz. `run\_start\_ms` her koşuda producer'ın başlangıç anıdır.

## Veri akışı

```
SUMO -TraCI-> producer -> \[traffic.raw] -> Spark -> \[traffic.metrics] -> controller
                                            |                                |
                                            +-> ClickHouse -> Grafana        |
   ^ setProgram <- producer <- \[traffic.control] <---------------------------+
```

## 1\. `traffic.raw`

Yazan: producer (Kişi A). Okuyan: Spark.
Sıklık: her 2 simülasyon saniyesinde, kavşak başına bir mesaj.

|Alan|Tip|Açıklama|
|-|-|-|
|`run\_id`|string|Koşunun benzersiz kimliği|
|`scenario`|string|Senaryo adı (öneri: `normal`, `demand\_surge`, `lane\_closure`)|
|`mode`|string|`fixed` veya `adaptive`|
|`seed`|int|Rastgele tohum|
|`sim\_time`|float|Simülasyon zamanı (s)|
|`event\_time\_ms`|int|`run\_start\_ms + sim\_time\*1000`|
|`produced\_at\_ms`|int|Mesajın producer tarafından üretildiği gerçek zaman|
|`tls\_id`|string|Sinyalli kavşak kimliği|
|`total\_halting`|int|Kavşağın tüm yaklaşım şeritlerindeki duran araç sayısı|
|`queue\_main`|int|Ana yön gruplarındaki duran araç sayısı|
|`queue\_side`|int|Yan yön gruplarındaki duran araç sayısı|
|`avg\_waiting\_time`|float|Yaklaşım şeritlerinde ortalama bekleme (s)|
|`avg\_speed`|float|Yaklaşım şeritlerinde ortalama hız (m/s)|
|`avg\_occupancy`|float|Ortalama doluluk (%)|
|`vehicle\_count`|int|Yaklaşım şeritlerindeki araç sayısı|

Örnek:

```json
{"run\_id":"r001","scenario":"normal","mode":"fixed","seed":1,"sim\_time":120.0,"event\_time\_ms":1767000120000,"produced\_at\_ms":1767000005123,"tls\_id":"J1","total\_halting":7,"queue\_main":5,"queue\_side":2,"avg\_waiting\_time":4.8,"avg\_speed":3.1,"avg\_occupancy":12.4,"vehicle\_count":18}
```

## 2\. `traffic.metrics`

Yazan: Spark (Kişi B). Okuyan: controller, ClickHouse.
Pencere: 10 simülasyon saniyesi, `run\_id` + `tls\_id` kırılımında.

|Alan|Tip|Açıklama|
|-|-|-|
|`run\_id`|string||
|`tls\_id`|string||
|`window\_start\_ms`|int|Pencere başlangıcı (`event\_time\_ms` ekseninde)|
|`window\_end\_ms`|int|Pencere bitişi|
|`sim\_time\_end`|float|Pencerenin bittiği simülasyon zamanı (s)|
|`last\_produced\_at\_ms`|int|Penceredeki en son `produced\_at\_ms` (gecikme ölçümü için)|
|`spark\_out\_at\_ms`|int|Spark'ın sonucu yazdığı gerçek zaman|
|`avg\_queue`|float|Pencere ortalaması `total\_halting`|
|`avg\_queue\_main`|float|Pencere ortalaması `queue\_main`|
|`avg\_queue\_side`|float|Pencere ortalaması `queue\_side`|
|`avg\_wait`|float|Pencere ortalaması `avg\_waiting\_time` (s)|
|`avg\_speed`|float|Pencere ortalaması `avg\_speed` (m/s)|
|`avg\_occupancy`|float|Pencere ortalaması `avg\_occupancy` (%)|

Spark yalnızca bu ortalamaları hesaplar. Düşük/orta/yüksek sınıflaması controller'da yapılır (analiz ve kontrol katmanları ayrı).

## 3\. `traffic.control`

Yazan: controller (Kişi B). Okuyan: producer (Kişi A), sinyal planını uygulayıcı.

|Alan|Tip|Açıklama|
|-|-|-|
|`run\_id`|string||
|`tls\_id`|string||
|`program\_id`|string|Uygulanacak sinyal programı (`0` sabit/varsayılan, `1` ana yön öncelikli, `2` yan yön öncelikli)|
|`decided\_at\_ms`|int|Controller'ın kararı verdiği gerçek zaman|
|`source\_produced\_at\_ms`|int|Kararın dayandığı pencerenin `last\_produced\_at\_ms` değeri|
|`sim\_time`|float|Kararın dayandığı pencerenin `sim\_time\_end` değeri|
|`reason`|string|Kısa gerekçe (örn. `high\_queue\_main`, `return\_to\_default`)|

Plan uygulandıktan sonra producer `applied\_at\_ms` ve `applied\_sim\_time` değerlerini kaydeder. Bu iki alan `traffic.control` mesajında yoktur, producer bunları doğrudan ClickHouse `control\_events` tablosuna yazar.

## Gecikme ölçümü

Gerçek zaman damgaları sırayla:

`produced\_at\_ms` (producer) → `spark\_out\_at\_ms` (Spark) → `decided\_at\_ms` (controller) → `applied\_at\_ms` (producer, plan uygulandığında)

Toplam gecikme = `applied\_at\_ms - source\_produced\_at\_ms`. Aşama payları da bu damgalardan ayrı ayrı hesaplanır. Simülasyon zamanı gecikmesi: `applied\_sim\_time - sim\_time`.

## Kararlar

1. **Mesaj biçimi JSON.** Python (`kafka-python`) ve Spark (`from\_json`) ile doğrudan çalışır, ek şema aracı gerekmez.
2. **Senaryo adları:** `normal`, `demand\_surge`, `lane\_closure`. Türkçe karakter ve boşluk kullanılmaz, dosya adlarında ve veritabanında sorun çıkarmaz.
3. **`total\_halting = queue\_main + queue\_side`.** Yaklaşım şeritlerinin tamamı `config/approaches.yaml` içinde ana ya da yan gruba atanır. Hiçbir şerit grupsuz kalmaz, böylece toplam her zaman tutarlıdır.
4. **`tls\_id`:** haritadan çıkan gerçek SUMO kavşak kimlikleri. Harita hazır olunca Kişi A bu belgeye ekler.
5. **Birim ve zaman kuralları** yukarıdaki "Genel kurallar" tablosundaki gibi: m/s, saniye, % doluluk, epoch ms.
6. **Örnekleme aralığı 2 simülasyon saniyesi.** 10 saniyelik pencerede yaklaşık 5 örnek düşer, ortalama için yeterlidir. Gerekirse sonra `.env` üzerinden değiştirilebilir, sözleşme değişmez.

