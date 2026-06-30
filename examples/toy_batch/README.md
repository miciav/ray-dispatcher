# toy_batch

Stima π con Monte Carlo su 4 configurazioni diverse, eseguite in parallelo su 2 VM Multipass.

Dimostra il flusso completo di `ray-dispatcher`: provisioning remoto, dispatch parallelo, raccolta risultati, teardown.

## Prerequisiti

- [Multipass](https://multipass.run/) installato e in esecuzione
- Una chiave SSH pubblica in `~/.ssh/` (usata per accedere alle VM)
- [uv](https://docs.astral.sh/uv/) installato

## Struttura

```
toy_batch/
  pyproject.toml        # dipendenze: ray-dispatcher + multipass-sdk
  run_batch.py          # driver: crea VM, dispatcha job, stampa risultati, distrugge VM
  experiment/           # progetto rsyncato sulle VM
    experiment.py       # stima π via Monte Carlo, scrive result.json
    pyproject.toml
    uv.lock
  configs/
    run_01.json         # {"n_samples": 100000, "seed": 42}
    run_02.json         # {"n_samples": 500000, "seed": 123}
    run_03.json         # {"n_samples": 1000000, "seed": 7}
    run_04.json         # {"n_samples": 200000, "seed": 99}
```

## Esecuzione

```bash
cd examples/toy_batch
uv run python run_batch.py
```

Il primo avvio scarica le dipendenze nel venv locale e richiede ~5-10 minuti per il provisioning delle VM.

## Cosa succede

1. **Lancio VM** — vengono create 2 VM Ubuntu 22.04 (`rd-toy-0`, `rd-toy-1`) con rsync e la tua chiave SSH
2. **Provisioning** — `ray-dispatcher` installa uv + Python 3.10 su ogni VM e sincronizza `experiment/`
3. **Dispatch parallelo** — 4 job partono contemporaneamente (2 VM × 2 slot); ogni job riceve il proprio file di configurazione via rsync
4. **Raccolta risultati** — `result.json` viene scaricato da ogni VM al termine
5. **Teardown** — le VM vengono distrutte automaticamente (anche in caso di errore)

## Output atteso

```
Launching 2 VMs...
  rd-toy-0: ready at 192.168.64.10
  rd-toy-1: ready at 192.168.64.11
Scanning host keys...

Dispatching 4 jobs across 2 VMs (2 slots each)...

All done in 47.3s

Job        Status       Result
------------------------------------------------------------
run_01     succeeded    pi ≈ 3.143920  (100,000 samples)
run_02     succeeded    pi ≈ 3.141116  (500,000 samples)
run_03     succeeded    pi ≈ 3.141396  (1,000,000 samples)
run_04     succeeded    pi ≈ 3.142040  (200,000 samples)

Destroying VMs...
  rd-toy-0: deleted
  rd-toy-1: deleted
```

La stima migliora all'aumentare di `n_samples`. Le run successive sono più veloci perché `ray-dispatcher` rileva che l'ambiente non è cambiato e salta `uv sync`.

## Personalizzare

Per aggiungere configurazioni basta creare nuovi file JSON in `configs/` con i campi `label`, `n_samples` e `seed`. Per cambiare il numero di VM o di slot paralleli, modifica `N_VMS` e `SLOTS_PER_VM` in `run_batch.py`.
