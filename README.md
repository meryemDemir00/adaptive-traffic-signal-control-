# Gerçek Zamanlı Büyük Veri Tabanlı Adaptif Trafik Sinyal Kontrol Sistemi

Yazılım Geliştirme – I, 2. Proje | Kocaeli Üniversitesi

## Proje Özeti

SUMO ile simüle edilen gerçek bir yol ağındaki (OpenStreetMap, Kocaeli / İzmit) trafik verisi Kafka ve Spark Structured Streaming ile gerçek zamanlı işlenir. Eşik tabanlı bir karar servisi, kavşaklardaki yoğunluğa göre sinyal planını seçer ve kararı simülasyona geri gönderir. Sabit zamanlı kontrol ile adaptif kontrol aynı ağ, aynı talep ve aynı rastgele tohumlarla karşılaştırılır. Makine öğrenmesi veya pekiştirmeli öğrenme kullanılmaz.

## Mimari

```
SUMO ─TraCI─> producer ──> [traffic.raw] ──> Spark ──> [traffic.metrics] ──> controller
   ^              ^                              └──> ClickHouse ──> Grafana        │
   └─ setProgram ─┴────────── [traffic.control] <────────────────────────────────────┘
```

Analiz ve kontrol katmanları ayrıdır: Spark yalnızca pencere bazlı göstergeleri hesaplar, kararı controller servisi verir.

| Servis | Görev |
|---|---|
| `producer` | SUMO'yu TraCI ile çalıştırır, kavşak bazlı ölçümleri `traffic.raw` topic'ine yazar, `traffic.control` komutlarını okuyup sinyal planını uygular. |
| `kafka` | Olay akışı (Apache Kafka 3.9.0, KRaft modu, tek broker). |
| `kafka-init` | `traffic.raw`, `traffic.metrics` ve `traffic.control` topic'lerini açılışta oluşturur, sonra kapanır. |
| `spark` | Spark Structured Streaming (3.5.8, `local[2]`). `traffic.raw` verisini 10 saniyelik pencerelerde işler, sonucu `traffic.metrics` topic'ine ve ClickHouse'a yazar. |
| `controller` | `traffic.metrics` topic'ini okur, eşik kurallarını uygular, kararı `traffic.control` topic'ine yazar. |
| `clickhouse` | Zaman serisi ve analitik depolama (24.8). |
| `grafana` | Gerçek zamanlı dashboard (Grafana OSS 11.3.0, ClickHouse eklentisi). |

Mesaj alanları, tipler ve birimler için [docs/messages.md](docs/messages.md) dosyasına bakın.

## Gereksinimler

- Docker Desktop (Windows'ta WSL2 ile) ve Docker Compose v2
- Git
- WSL2'ye en az 6 GB bellek ayrılması önerilir. Windows'ta `C:\Users\<KULLANICI>\.wslconfig` dosyası:
  ```
  [wsl2]
  memory=6GB
  processors=4
  ```
  Değişiklikten sonra `wsl --shutdown` çalıştırıp Docker Desktop'ı yeniden başlatın.
- Spark ilk açılışta Kafka paketini indirir, bu nedenle ilk çalıştırmada internet bağlantısı gerekir.

Yerel geliştirme için (isteğe bağlı): Python 3.11 ve SUMO 1.28.0 (sumo.dlr.de).

## Hızlı Başlangıç

```bash
git clone <repo-adresi>
cd <repo-klasoru>

# Ortam dosyasını oluşturun (Windows PowerShell: Copy-Item .env.example .env)
cp .env.example .env

# Altyapı, Spark ve controller servislerini başlatın
docker compose up -d --build

# Servislerin durumuna bakın (kafka-init işini bitirip "Exited (0)" olur)
docker compose ps
```

Tüm yapılandırma değerleri (topic adları, ClickHouse bağlantısı, pencere süresi, kontrol parametreleri) `.env` dosyasından okunur, kodda gömülü değildir. Örnek değerler `.env.example` içindedir.

Erişim adresleri:

| Servis | Adres |
|---|---|
| Grafana | http://localhost:3000 (varsayılan kullanıcı `admin`, şifre `admin`; ilk girişte değiştirme adımı atlanabilir) |
| ClickHouse (HTTP) | http://localhost:8123 |
| Kafka (bilgisayardan erişim) | `localhost:29092` |

Grafana'da **Adaptif Trafik Sinyal Kontrolu** dashboard'u ve ClickHouse veri kaynağı dosyadan otomatik yüklenir, elle ayar gerekmez.

## Simülasyon Koşusu Çalıştırma

Senaryolar: `normal`, `demand_surge` (ani talep artışı), `lane_closure` (şerit kapanması). Kontrol modları: `fixed` (sabit zamanlı) ve `adaptive` (eşik tabanlı).

```bash
docker compose --profile sim run --rm producer \
  --run-id r001 --scenario demand_surge --mode adaptive --seed 1 --step-delay 0.05
```

Önemli kurallar:

- **Her koşudan önce Spark yeniden oluşturulmalıdır.** Pencereler simülasyon zamanıyla çalıştığı için Spark'ın watermark saati bir koşunun sonunda ileride kalır; yeniden oluşturulmazsa sonraki koşunun başı geç veri sayılıp tabloya yazılmaz.
  ```bash
  docker compose up -d --force-recreate spark
  # yaklaşık 60 saniye bekleyin, sonra koşuyu başlatın
  ```
- Karşılaştırmanın adil olması için sabit ve adaptif koşularda aynı senaryo, aynı seed ve aynı `--step-delay` değeri kullanılmalıdır.
- Aynı `run_id` ikinci kez kullanılmamalıdır.

Koşu çıktıları `experiments/results/` klasörüne yazılır.

## Karar Mantığı (Eşik Tabanlı Kontrol)

Spark her kavşak için 10 simülasyon saniyelik pencerede `avg_queue` (kuyruk), `avg_wait`, `avg_speed` ve `avg_occupancy` değerlerini hesaplar. Controller kuyruk değerine göre yoğunluk düzeyini belirler:

- **Eşikler tahminle değil veriyle belirlenmiştir.** Normal senaryonun sabit zamanlı baseline koşularından (3 farklı seed) her kavşak için kuyruk dağılımı çıkarılmış; düşük yoğunluk P50'ye kadar, orta yoğunluk P50 ile P85 arası, yüksek yoğunluk P85'in üstüdür.
- **Hysteresis:** Yüksek düzeye girmek için kuyruk P85'i aşmalı, çıkmak için `0.8 × P85` değerinin altına inmelidir.
- **Ardışık pencere:** Yüksek yoğunluk art arda `CONTROL_CONSECUTIVE_WINDOWS` (varsayılan 3) pencere sürerse plan değişir.
- **Minimum kontrol aralığı:** Bir kavşakta iki plan değişikliği arasında en az `CONTROL_MIN_INTERVAL_SECONDS` (varsayılan 30) simülasyon saniyesi geçer.
- **Plan seçimi:** Ana yön kuyruğu yan yön kuyruğunun `dominance_ratio` (1,5) katından büyükse program `1` (ana yön öncelikli), yan yön büyükse program `2` (yan yön öncelikli) seçilir. Yoğunluk düşünce varsayılan program `0` (sabit zamanlı plan) tekrar devreye girer.
- **Sabit zamanlı modda** controller karar yazmaz, ancak veri hattı aynı şekilde çalışır; böylece iki modun sistem yükü eşit olur.

Eşik değerleri `config/thresholds.yaml` dosyasındadır. Ana ve yan yön şerit grupları `config/approaches.yaml` içinde tanımlıdır.

| Kavşak | Orta sınırı (P50) | Yüksek girişi (P85) | Yüksek çıkışı (0,8 × P85) |
|---|---|---|---|
| B0 | 0 | 8,6 | 6,9 |
| C0 | 0 | 12,06 | 9,6 |
| C1 | 0 | 8,0 | 6,4 |

Normal senaryoda pencerelerin yarısından fazlasında kuyruk oluşmadığı için P50 sıfırdır. Controller yalnızca yüksek eşiği kullanır.

## Dashboard ve Ölçümler

Grafana panelleri (üstteki listeden koşu seçilir):

- Üç kavşağın anlık durumu (kuyruk, bekleme, hız, seçili sinyal planı)
- Kuyruk uzunluğu ve ortalama bekleme süresi grafikleri
- Seçili sinyal planı ve kontrol değişikliklerinin zaman çizelgesi
- Spark aşaması ve uçtan uca gecikme
- Koşu bazlı sistem metrikleri ve tüm koşuların karşılaştırma tablosu

Gecikme zinciri dört gerçek zaman damgasıyla kaydedilir: `produced_at_ms` (producer) → `spark_out_at_ms` (Spark) → `decided_at_ms` (controller) → `applied_at_ms` (plan uygulandığında). Uçtan uca gecikme `applied_at_ms − source_produced_at_ms` olarak hesaplanır.

ClickHouse (`traffic` veritabanı):

| Nesne | İçerik |
|---|---|
| `metrics` | Spark'ın pencere sonuçları (kavşak, koşu, senaryo, mod, göstergeler, kayıt sayısı) |
| `control_events` | Controller kararları ve planın uygulandığı an |
| `run_system_metrics` (görünüm) | Koşu başına işlenen kayıt/saniye, veri akışı sürekliliği, en uzun boşluk, karar sayısı ve gecikme özeti |
| `raw_samples` | Ham ölçümler için ayrılmış tablo, mevcut sürümde doldurulmaz |

Koşu özetini komut satırından görmek için:

```bash
docker exec clickhouse clickhouse-client --user <CLICKHOUSE_USER> --password <CLICKHOUSE_PASSWORD> \
  -q "SELECT * FROM traffic.run_system_metrics FORMAT Vertical"
```

Kullanıcı adı ve şifre `.env` dosyasındaki değerlerdir.

## Klasör Yapısı

```
.
├── docker-compose.yml        # Tüm servislerin tanımı
├── .env.example              # Yapılandırma şablonu (.env olarak kopyalanır)
├── requirements-dev.txt      # Yerel geliştirme için Python paketleri
├── clickhouse/init.sql       # ClickHouse tabloları ve görünüm
├── config/
│   ├── thresholds.yaml       # Kavşak bazlı eşikler ve kontrol parametreleri
│   └── approaches.yaml       # Ana/yan yön şerit grupları
├── docs/messages.md          # Kafka mesaj sözleşmesi
├── experiments/results/      # Koşu çıktıları
├── grafana/provisioning/     # Veri kaynağı ve dashboard (otomatik yüklenir)
├── services/
│   ├── producer/             # SUMO TraCI → Kafka, plan uygulayıcı
│   ├── spark_job/            # Spark Structured Streaming işi
│   └── controller/           # Eşik tabanlı karar servisi
└── sumo/                     # Ağ, rotalar ve senaryo dosyaları
```

## Yerel Geliştirme (İsteğe Bağlı)

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
```

SUMO 1.28.0 yerelde kurulu olmalıdır (`sumo --version`, `netconvert --version`). Yerelden çalışan bir producer, Docker'daki Kafka'ya `localhost:29092` üzerinden bağlanır.

## Sorun Giderme

| Belirti | Çözüm |
|---|---|
| `failed to connect to the docker API` | Docker Desktop açık değil, uygulamayı başlatın. |
| `spark` sürekli `Restarting` | `docker compose logs spark --tail 40` ile hata satırına bakın. Hesap şeması değiştiyse `docker compose up -d --force-recreate spark` ile sıfırlayın. |
| Koşunun başındaki pencereler tabloda yok | Spark koşudan önce yeniden oluşturulmamıştır (Simülasyon Koşusu Çalıştırma bölümüne bakın). |
| ClickHouse `Authentication failed` | `.env` içindeki `CLICKHOUSE_USER` ve `CLICKHOUSE_PASSWORD` değerleri ClickHouse'un ilk oluşturulduğu değerlerle aynı olmalıdır. |
| Grafana'da `No data` | Üstteki **Kosu (run_id)** listesinden bir koşu seçildiğinden emin olun. |

## Harita, Veri ve Üçüncü Taraf Yazılımlar

- **Yol ağı:** OpenStreetMap, © OpenStreetMap contributors (ODbL lisansı). Kocaeli / İzmit'te, aynı arter üzerinde birbirini etkileyen üç sinyalli kavşak (`B0`, `C0`, `C1`) seçilmiş ve SUMO ağına dönüştürülmüştür.
- **Trafik talebi:** Gerçek sayım verisi kullanılmamıştır; talep kontrollü ve tekrarlanabilir biçimde sentetik üretilmiştir.
- **Yazılımlar:** Eclipse SUMO (EPL-2.0), Apache Kafka, Apache Spark, ClickHouse, Grafana, Docker.
- **Python kütüphaneleri:** traci, sumolib, kafka-python, clickhouse-connect, PyYAML, python-dotenv, pandas, matplotlib, numpy.
