# TODO

Gap noti nell'implementazione del protocollo Kafka, in ordine di rilevanza. Vedi
la sezione "Supported Kafka Protocol Versions & Features" in `readme.md` per
cosa è già implementato.

## Gap funzionali

- [x] **`AddOffsetsToTxn` (api key 25)**: implementato (`protocol/transactions.cr`,
  `Client#add_offsets_to_txn`). `Producer#send_offsets_to_transaction` ora
  registra il gruppo via `AddOffsetsToTxn` (una sola volta per gruppo per
  transazione, tracciato in `@txn_groups`) prima di `TxnOffsetCommit`. Test
  end-to-end via MockBroker in `spec/consumer_producer_spec.cr`.

- [x] **Fallback al consumer group classico**: implementato. `Consumer#each`
  tenta prima KIP-848 (`ConsumerGroupHeartbeat`); su `UNSUPPORTED_VERSION`
  (error 35) cade automaticamente su `JoinGroup`/`SyncGroup`/`Heartbeat`/
  `LeaveGroup` classici. Nuovo `protocol/consumer_protocol.cr` implementa la
  serializzazione del "consumer embedded protocol" (`ConsumerProtocolSubscription`/
  `Assignment`, usato dentro ai byte opachi di JoinGroup/SyncGroup) e un
  `RangeAssignor` (algoritmo di default di Kafka) usato dal membro eletto
  leader per calcolare l'assegnazione di tutti i membri. `JoinGroupRequest`/
  `Response` e `SyncGroupRequest` sono stati estesi per portare
  subscription/assignment reali invece di byte vuoti. Test end-to-end via
  MockBroker in `spec/kip_e2e_spec.cr`.

  Limite noto: come il resto di `Consumer#each`, l'assegnazione classica
  copre solo il primo topic sottoscritto (stessa semplificazione già
  presente per KIP-848).

- [x] **`OffsetForLeaderEpoch` (api key 23)**: implementato (`protocol/offset_for_leader_epoch.cr`
  v2, `Client#offset_for_leader_epoch`). `Metadata` ora cattura e traccia il
  `leader_epoch` per partizione (`Client#leader_epoch_for`), inviato in ogni
  `FetchRequest` come `current_leader_epoch` (era hardcoded a `-1`). Il path
  di retry di `Client#fetch` rileva un cambio di epoch dopo un
  `NOT_LEADER_OR_FOLLOWER`, interroga `OffsetForLeaderEpoch` e riavvolge
  l'offset di fetch se rileva troncamento, prima di ritentare. Stesso fix di
  correttezza applicato a `Client#start_prefetch` (derivava l'offset
  successivo incrementando alla cieca invece che da `record.offset + 1`).
  Test end-to-end via MockBroker in `spec/mock_broker_spec.cr`.

- [x] **Sessioni Fetch incrementali (KIP-227)**: implementato. Nuovi
  `Protocol::FetchSessionRequest`/`Response` (`protocol/produce_fetch.cr`) e
  `Client#fetch_many(topic, offsets)`, che raggruppa tutte le partizioni di
  un topic per broker (`closest_replica_connection_for_partition`) in
  un'unica `FetchRequest` per broker, invece di una per partizione. Lo stato
  di sessione (`session_id`/`session_epoch`/set di partizioni tracciate) vive
  su `Connection` (`fetch_session_*`), safe senza lock aggiuntivi perché
  `send_request`/`read_response` bracketano già un ciclo completo sotto
  mutex. `Consumer#each` (sia KIP-848 che fallback classico) ora usa un
  singolo fetch loop consolidato (`spawn_multi_fetcher`) al posto di una
  fiber per partizione — rilegge `@assigned_partitions` a ogni tick, quindi
  segue i rebalance senza bookkeeping per-partizione. L'assegnazione manuale
  (`Consumer#assign`) resta invece sul modello a fiber-per-partizione
  esistente, perché può coprire più topic contemporaneamente (fuori
  dallo scope a singolo-topic di `fetch_many`). Verificato che `crystal spec`
  passa invariato con la sola sostituzione del loop di fetch (nessuna
  modifica ai mock esistenti necessaria, perché il wire format è identico a
  quello non-incrementale, solo con più partizioni per richiesta).

## Fuori scope (per progetto, non per dimenticanza)

Nessuna API amministrativa — coerente con lo scope dichiarato in
`CLAUDE.md` (libreria purpose-built per `cryspace`, non per uso
general-purpose):

- `CreateTopics` / `DeleteTopics` / `CreatePartitions`
- `DescribeConfigs` / `AlterConfigs` / `IncrementalAlterConfigs`
- ACL: `DescribeAcls` / `CreateAcls` / `DeleteAcls`
- `ListGroups` / `DescribeGroups` / `DeleteGroups`
- Delegation token: `Create/Renew/Expire/DescribeDelegationToken`
- `AlterReplicaLogDirs` / `DescribeLogDirs`
- `AlterPartitionReassignments` / `ListPartitionReassignments`
- Quote: `DescribeClientQuotas` / `AlterClientQuotas`
