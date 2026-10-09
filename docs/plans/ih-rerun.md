# Korrigierte IH-Serie auf dem Workstation-Rechner

> Historical design/implementation record. Status statements and commands refer
> to the work at that time, not current release instructions. See the
> [README](../../README.md) for current usage; preserve protocol details when
> interpreting archived measurements.

**Auftrag:** Umsetzung und Ausführung von diesem Repository aus vorbereiten. Dieser Plan wurde angefordert; Codeänderungen und Messstart sind noch nicht erfolgt. Umsetzung und Pilot/Hauptmessung erst mit Eikes Umsetzungs-/Startauftrag; danach ist der bestandene Pilot das technische Gate für die Hauptserie. Paperübernahme bleibt ein separater Schritt. Keine Commits, Uploads oder Änderungen historischer Messdateien; niemals `SHA256SUMS` erzeugen.

## 1. Ziel und Arbeitsbasis

Neue, innerhalb jeder Engine vergleichbare Insert-Heavy-Ergebnisse statt der bisherigen problematischen IH-Kontraste. Gleicher Rechner verbessert den Anschluss an die Workstation-Daten, beweist aber keine historische Setup-Identität oder Ursache alter Unterschiede.

Arbeitsbasis ist der aktuelle Checkout auf `main`, HEAD `6cb737bff35eb1b1d509b20cd4b8900f93c51c06`. Kein Zurücksetzen auf historische Revisionen. Gegen Referenz `fa8a9590bc9a7ff17a8a6f8f9142d2e299452298` sind CLI, Workload, Runner und Mixed-Implementierungen unverändert; im geprüften Engine-/Docker-Bereich unterscheiden sich nur Mongo-Port, Bereitschafts-Timeouts und eine semantisch unveränderte MySQL-Metrikberechnung samt Kommentaren. Cassandra verwendet hier weiterhin **bucket=1**, nicht das neuere Bucket-Schema der veröffentlichten Artefaktversion. Bei Beginn erneut Status/Diff prüfen; vorhandene ungetrackte CSVs und Binaries erhalten.

Das alte `docs/paper-context.md` beschreibt frühere Paperstände und ist keine aktuelle Claim-/Statistikvorgabe. Aktueller Paperstatus: `../uuid-paper/paper-writing.md`; ausführliche Begründung: `../uuid-paper/drafts/mixed-ih-synthesis-and-rerun-scope.md`.

## 2. Festes Zielprotokoll

| Parameter | Festlegung |
|---|---|
| Engines | PostgreSQL, MySQL, MongoDB, Cassandra; einzeln nacheinander |
| Szenario | nur `mixed-insert-heavy`, niemals `all` |
| Je Lauf | frischer Datenbestand, 100.000 verifizierte Preload-Zeilen, 200.000 Mixed-Operationen |
| Mischung | zufällige 70 % Insert / 30 % Read, keine Updates |
| Parallelität | ein Client / eine Operation gleichzeitig |
| Schemes | Sequential, UUIDv1, UUIDv7, ULID, monotonic ULID, UUIDv4; MongoDB zusätzlich ObjectId |
| Wiederholungen | fünf je Scheme: 35 MongoDB + 3 × 30 = **125 Hauptläufe** |
| Read-Ziele | gleichverteilt aus der vollständigen, eindeutigen **festen Preload-Liste**; keine neu eingefügten IDs aufnehmen |
| Inserts | ausschließlich neue Identitäten; MongoDB-/Cassandra-Sequential oberhalb des verifizierten Preload-Maximums fortsetzen |
| Reihenfolge | je Engine fünf Blöcke; jeder Block enthält jedes Scheme einmal, vorab mit gespeichertem Seed gemischt |

Read-Zielvereinheitlichung und Reihenfolge sind bewusste Protokolländerungen, nicht nur ein Zählerfix. Vor dem Pilot bestätigen und anschließend nicht ergebnisabhängig ändern. Vorladen bleibt im bestehenden Batch-100-Pfad; Mixed-Inserts sind Einzeloperationen. Key-Typen, Generatoren und Engine-spezifische Payload-Erzeugung des Arbeitsstands erhalten, exakt dokumentieren; keine zusätzliche Payload- oder Schemaoptimierung. Bestehende Limits: 4 CPUs/8 GB je Container, Cassandra 4G Heap/1G NewGen; tatsächliche Anwendung prüfen.

Keine reinen Insert-/Read-/Update-, RU-, Acht-Client-, 1M-/10M- oder Cluster-Nachmessungen. Die Hauptserie umfasst kumuliert 12,5 Mio. Preload-Zeilen und 25 Mio. Mixed-Operationen, nicht gleichzeitig gespeicherte Daten dieser Größe.

## 3. Implementierung und Nachweise

Eng begrenzte Änderungen an `cmd/workload/main.go`, `cmd/benchmark/main.go`, Mixed-Runnern/-Adaptern einschließlich PostgreSQL/pgbench, Parser/Export und Tests. Gemeinsame Pfade nicht unbeabsichtigt für andere Szenarien ändern. Ein Launcher muss Scheme-Auswahl, Blockfolge, Seeds und eindeutige Lauf-IDs wirklich unterstützen; der bisherige Standardaufruf allein führt die geplante Reihenfolge nicht aus.

Pro Lauf maschinenlesbar exportieren: Engine/Scheme/Block, Start/Ende und Phasendauern, angeforderte und tatsächliche Operationszahlen, Insert-/Read-Versuche und Erfolge, Fehlerarten, nicht gefundene Reads, Preload-/Endkardinalität, Sequential-Bereich, Zahl eindeutiger Read-Ziele, Mixed-Durchsatz und verfügbare Latenzmetriken. Auch pgbench muss die Gültigkeit seiner Reads belegen; erfolgreiche SQL-Ausführung allein reicht nicht. Zähler dürfen nicht bloß aus den Gewichten 70/30 geschätzt werden. Prüfungen und teure Abfragen außerhalb des Zeitfensters; notwendige Inline-Instrumentierung innerhalb einer Engine für alle Schemes identisch.

**Gültigkeitsbedingungen:** Preload exakt 100.000; Zielliste exakt 100.000 eindeutige vorhandene IDs; genau 200.000 abgeschlossene Mixed-Operationen; keine Fehler oder Read-Misses; Endbestand = Preload + unabhängig erfasste erfolgreiche Inserts. Etwa 240.000 Endzeilen sind eine Erwartung, kein fixes Soll bei zufälliger Mischung. Cassandra-Upserts erfordern ausdrücklich den Kardinalitätsnachweis. Keine zusätzliche Warmup-Read-Runde nur zur Prüfung; vollständige Zielerfassung und Kontrollen bei allen Schemes identisch durchführen.

Tests: Counter-Grenze nach Preload, unveränderliche Zielliste, Zählerabgleich, absichtlich eingebrachte Duplikate/Read-Misses, Parser-/CSV-Roundtrip einschließlich pgbench, bestehende Go-Tests und beide Builds. Pilot: je Engine einmal Sequential und UUIDv4 im vollen Umfang (**acht zusätzliche Läufe**, nicht Teil von n=5). Korrektheit, Instrumentierung und vollständige Wallclock anhand dieses Piloten prüfen; offene Nachweislücken verhindern den Hauptlauf.

## 4. Sicher ausführen und abbrechen

Eine neue Kampagnen-ID, beispielsweise `ih-corrected-<timestamp>`, und ein eigener Ordner `results/<campaign-id>/` mit Manifest, Quellsnapshot inklusive neuer Dateien/Diff, lokal erhaltenen Binaries/Buildinfos, Logs, Pilotdaten, per-run Rohdaten und abgeleiteten CSVs. Manifest vorab schreiben: Protokoll, komplette Befehle, Reihenfolge/Seeds, Base-Revision, Payloadbeschreibung, Schema/Generatoren, DB-/Treiber-/Toolversionen, Image-IDs/Digests, Ressourcen und Hostzustand. Keine Geheimnisse exportieren.

Lokaler Ryzen 7 7840U/32 GB/NVMe passt zu den alten Eckdaten; Kernel jetzt 7.2.7 statt dokumentierter 6.18. Images sind vorhanden, die Compose-Dateien verwenden `mongo:8`, `cassandra:5`, `mysql:8-debian`, `uuid-benchmark-postgres:18`. Genau diese lokalen Images fest binden; kein stiller Pull/Rebuild. Versionen/Extensions nach Start erfassen. Freien Plattenplatz, Netzbetrieb, verhinderten Suspend und ruhige Hostlast prüfen. Fremde Dienste nicht eigenmächtig stoppen. Container/Volumes eindeutig dieser Kampagne zuordnen; Start-/Cleanup-Code mit `down -v` darf ausschließlich deren Ressourcen treffen.

Bei Fehler, Kardinalitätsabweichung oder unvollständiger Messung anhalten, Daten/Grund bewahren und prüfen, nicht still neu versuchen. Ein eindeutiger Infrastrukturfehler vor Beginn der Mixed-Phase erlaubt nach dokumentierter Behebung höchstens einen Wiederholungsversuch derselben Konfiguration. Keine ausreißerabhängigen Ausschlüsse. Code-/Protokolländerungen nach Pilot erfordern erneute Prüfung; während der Hauptserie neuer Kampagnenstand statt Vermischung. Zeitlimit und Umgang mit Unterbrechung vor Start festlegen; Teilserien nicht als vollständiges n=5 ausgeben.

## 5. Zeit und Übernahme

Historische Workstation-Durchsätze ergeben für 200.000 Operationen je Hauptlauf **3 h 47 min reine Mixed-Zeit**: MongoDB 7,6 min, Cassandra 121,4 min, MySQL 78,6 min, PostgreSQL 18,9 min. Vorsichtiges Hauptlaufbudget inklusive Starts, Vorladen, Kontrollen und veränderter Kosten: **5–9 Stunden**. Vorbereitung/Tests/Pilot etwa 2–4 Stunden, anschließende Auswertung/Integration 1–3 Stunden. Der Pilot entscheidet, ob dieses Budget trägt; keine Übernachtgarantie.

Vor Sichtung der Hauptresultate gilt: alle gültigen Schemes verwenden, unabhängig von Richtung oder Größe. Auswertung weiterhin deskriptiv: fünf Rohwerte, Median, 100 × Median(Scheme)/Median(neuer Sequential-Baseline). Keine neue/alte Baseline mischen und keine neue Signifikanzentscheidung automatisch einführen.

**Paperwerte ersetzen, historische Rohdaten nicht überschreiben.** Neue IH-CSV-Dateien separat erzeugen, tatsächlichen Preload und Operationsumfang eindeutig ausweisen. Paper-/Artefaktskripte später ausdrücklich auf diese Quelle umstellen; falls ein kombiniertes CSV nötig ist, nur als neu benannte abgeleitete Datei mit Herkunftszuordnung. Andere Szenarien und alte CSVs unverändert lassen. Nach Prüfung sämtliche IH-Makros, Tabellen, Textaussagen, Methoden, Artefaktreferenzen und Reproduktionsprüfungen konsistent aktualisieren; Zeitpunkt und Grund der Ersetzung dokumentieren. Gleicher Host erlaubt keine kausale Alt-/Neu-Erklärung und keinen isolierten Workload-Mix-Effekt. Solche Aussagen würden andere Kontrollen verlangen.
