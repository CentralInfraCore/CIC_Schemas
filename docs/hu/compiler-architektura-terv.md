# Unified Compiler Architektúra Terv

**Státusz:** Tervezési dokumentum — Implementációs célkitűzés  
**Verzió:** 1.0  
**Branch:** schemas/devel  

---

## 1. Architektúra áttekintés

### Cél

Ennek a tervnek a célja az `CIC-Schemas` (v18) ad-hoc, monolit compilerének és a `base-repo` tiszta, moduláris infrastruktúrájának egyesítése. Az eredmény egy egységes `compiler.py` + könyvtár stack, amely képes minden repo-típust kezelni (séma repók, workflow repók, modul/relay repók), miközben mindkét kódbázis legjobb tulajdonságait megőrzi.

### Tervezési elvek

1. **A `compiler.py` csak egy vékony CLI diszpécser.** Minden logika könyvtárakban él.
2. **A `project.yaml` az univerzális projekt manifest** minden repo-típushoz. Soha nem tartalmaz `spec` blokkot.
3. **A `spec` tartalom dedikált forrásfájlokban él** (pl. `sources/index.yaml` séma repóknál).
4. **A konfiguráció adat-vezérelt** a `project.yaml` `compiler_settings` szekciójában. Nincsenek hardkódolt útvonalak vagy konstansok.
5. **Kettős aláírás modell**: fejlesztői aláírás (`sign`) + CIC central authority aláírás (`cicSign`/`cicSignedCA`). A central signing jelenleg az ideiglenes `finalize_release.py` szkripttel zajlik; a relay fogja felváltani.
6. **A validator integritása ellenőrzendő használat előtt** (v18-as minta): egy validator séma SHA-256 ellenőrző összegét a séma saját metaadatában tárolt értékkel kell összevetni, mielőtt bármit validálnánk vele.
7. **Kétszintű séma kiadás**: `release-dependency` validator sémákat állít elő a `dependencies/` könyvtárba; `release-schema` alkalmazás sémákat a `release/` könyvtárba.

### Modultérkép

```
tools/
├── compiler.py                   ← vékony CLI (base-repo mintája megmarad)
├── infra.py                      ← ReleaseManager (refaktorált, schemalib-et hív)
├── finalize_release.py           ← IDEIGLENES: törlendő, ha a relay elkészül
│
├── releaselib/                   ← marad ahogy van (base-repo)
│   ├── __init__.py
│   ├── exceptions.py             ← ReleaseError hierarchia
│   ├── vault_service.py          ← VaultService (sign, get_certificate)
│   └── git_service.py            ← GitService (branch, commit, tag, merge)
│
└── schemalib/                    ← ÚJ: v18 compiler.py-ból kiemelve/portolva
    ├── __init__.py
    ├── loader.py                 ← load_and_resolve_schema, convert_to_json_serializable
    ├── validator.py              ← get_validator_schema, integrity_check, run_jsonschema
    └── artifact.py              ← generate_signed_artifact, checksum, cert parsing
```

### Felelősségi mátrix

| Modul | Felelősség |
|---|---|
| `compiler.py` | CLI argumentum-feldolgozás, env-var olvasás, service példányosítás, parancs-diszpécsálás |
| `infra.py::ReleaseManager` | Git workflow-orchestráció, project.yaml életciklus, fázis-detektálás |
| `releaselib/vault_service.py` | Vault Transit aláírás, Vault KV tanúsítvány lekérés |
| `releaselib/git_service.py` | Összes subprocess git hívás, branch/tag/merge műveletek |
| `releaselib/exceptions.py` | Kivétel hierarchia |
| `schemalib/loader.py` | YAML betöltés, `$ref` feloldás, JSON-körforgás normalizálás |
| `schemalib/validator.py` | Validator séma lekérés, integritás ellenőrzés, jsonschema futtatás |
| `schemalib/artifact.py` | Aláírt artifact felépítés, checksum számítás, tanúsítvány parszolás |

---

## 2. `schemalib/` részletes terve

### 2.1 `schemalib/loader.py`

Ez a modul felváltja az `infra.py`-ban lévő jelenlegi `load_and_resolve_schema()` függvényt. A v18-as verzió lényegesen jobb, mert JSON körforgást hajt végre, amely eltávolítja a `JsonRef` proxy objektumokat és normalizálja a `datetime` példányokat minden további feldolgozás előtt.

**Függvények:**

```python
def convert_to_json_serializable(obj: Any) -> Any:
    """
    Rekurzívan átalakítja a Python objektumgráfot olyanná, amely teljes
    egészében JSON-serializálható. Kezeli a JsonRef proxykat (kényszerített
    feloldással), datetime objektumokat (ISO-8601 stringgé), és egyéb
    szélső eseteket. A JsonRef.replace_refs() után belső hívásra kerül.
    """

def load_and_resolve_schema(path: Path) -> dict:
    """
    Betölt egy YAML fájlt, feloldja az összes $ref hivatkozást (beleértve
    a keresztfájl hivatkozásokat a fájl könyvtárát alapuri-ként használva),
    majd JSON körforgást hajt végre a convert_to_json_serializable() segítségével
    annak garantálására, hogy a visszaadott objektum csak egyszerű Python
    típusokat tartalmaz.

    Raises:
        ConfigurationError: ha a fájl hiányzik vagy a YAML hibás.
    Returns:
        dict: Teljesen feloldott, JSON-serializálható dokumentum.
    """

def load_yaml(path: Path) -> Optional[dict]:
    """
    Betölt egy YAML fájlt $ref feloldás nélkül. Üres fájlok esetén None-t ad vissza.
    Raises ConfigurationError hiányzó fájl vagy parse hiba esetén.
    """

def write_yaml(path: Path, data: dict) -> None:
    """
    Atomikusan ír adatokat egy YAML fájlba temp-file + os.replace() minta
    segítségével. Raises ReleaseError I/O hiba esetén.
    """
```

**Adatfolyam:**

```
YAML fájl a lemezen
    → yaml.safe_load()
    → JsonRef.replace_refs(base_uri=file_dir/)   # $ref feloldás
    → convert_to_json_serializable()              # proxy típusok eltávolítása
    → json.loads(json.dumps(...))                 # körforgás normalizálás
    → egyszerű dict (hashelésre / validálásra kész)
```

### 2.2 `schemalib/validator.py`

Ez a modul séma validációs logikát tartalmaz, a v18-ból portolva, ahol a validáció a monolit compilerbe volt ágyazva.

**Függvények:**

```python
def get_validator_schema(
    validator_name: str,
    validator_version: str,
    dependencies_dir: Path,
) -> dict:
    """
    Betölt egy validator sémát a dependencies/ könyvtárból.
    A várt fájlnév minta: <name>-<version>.yaml

    Visszaadás előtt meghívja a verify_validator_integrity()-t, hogy
    megbizonyosodjon arról, hogy a sémát nem módosították.

    Raises:
        ConfigurationError: ha a fájl nem található.
        ValidationFailureError: ha az integritás ellenőrzés megbukik.
    """

def verify_validator_integrity(
    schema: dict,
    expected_checksum: str,
) -> None:
    """
    Ellenőrzi a validator séma spec blokkjának SHA-256 checksumját
    a séma saját metaadatában tárolt várható értékkel szemben.

    Ez egy biztonsági kontroll: egy manipulált validator csendben
    elfogadhatna érvénytelen sémákat.

    Raises:
        ValidationFailureError: ha a checksum nem egyezik.
    """

def run_validation(
    instance: dict,
    validator_schema: dict,
) -> None:
    """
    Futtatja a jsonschema.validate()-et az instance-ra a validator_schema['spec']
    segítségével. A JsonSchemaValidationError-t ValidationFailureError-rá csomagolja
    a konzisztens kivétel felületért.

    Raises:
        ValidationFailureError: validációs hiba esetén.
    """
```

**Integritás ellenőrzési algoritmus:**

1. A validator séma dict-ből kivonjuk a `spec` blokkot.
2. Kanonikus JSON-re serializáljuk (`json.dumps(sort_keys=True, separators=(',', ':'))`).
3. SHA-256 hash-eljük az UTF-8 byte-okat.
4. A hex digestet összehasonlítjuk a `schema['metadata']['checksum']`-mal.
5. Eltérés esetén `ValidationFailureError`-t dobunk.

### 2.3 `schemalib/artifact.py`

Ez a modul kezeli az aláírt release artifactok felépítését. Magába foglalja az `infra.py::_execute_developer_preparation_phase()`-ben és a `finalize_release.py`-ban jelenleg szétszórt logikát.

**Függvények:**

```python
def parse_certificate_info(pem_cert_data: str) -> tuple[str, str]:
    """
    Parszolja a PEM tanúsítványt pyOpenSSL segítségével.
    Kinyeri a Common Name-t és az email-t (SubjectAltName-ből vagy emailAddress-ből).
    Visszaad: (name: str, email: str)
    Parse hiba esetén ("Unknown", "unknown@example.com")-ra esik vissza.
    """

def compute_spec_checksum(spec: dict) -> str:
    """
    Kiszámítja a spec blokk kanonikus SHA-256 hex digestjét.
    A determinizmushoz json.dumps(sort_keys=True, separators=(',', ':'))-t használ.
    Visszaad: hex string (64 karakter)
    """

def build_signing_payload(
    name: str,
    version: str,
    checksum: str,
    build_timestamp: str,
) -> str:
    """
    Felépíti a kanonikus aláírás metadata dict base64-kódolt SHA-256
    digestjét. Ez a bemenet a VaultService.sign()-hoz.
    Visszaad: base64 string (Vault prehashed=True végpontjához alkalmas)
    """

def generate_signed_artifact(
    spec: dict,
    metadata_base: dict,
    release_version: str,
    build_timestamp: str,
    developer_cert: str,
    issuer_cert: str,
    signature: str,
) -> dict:
    """
    Összeállítja a teljes release artifact dict-et. Ez az a struktúra,
    amelyet release/<name>-<version>.yaml-ba írunk (séma repóknál).

    A visszaadott dict tartalmazza:
        metadata:
            name, version, checksum, sign, build_timestamp,
            createdBy: {name, email, certificate, issuer_certificate},
            buildHash: ""      # placeholder, a build lépés tölti ki
            cicSign: ""        # placeholder, a relay/finalize_release.py tölti ki
            cicSignedCA:
                certificate: ""  # placeholder
        spec: <a feloldott spec dict>
    """
```

---

## 3. `compiler.py` parancsok

### Parancs-készlet

| Parancs | Leírás | Alkalmazható repo típusok |
|---|---|---|
| `validate` | Offline validáció: forrásfájlok betöltése, $ref-ek feloldása, jsonschema futtatása validator sémával (integritás ellenőrzéssel) | Séma repók |
| `release` | Teljes Git-workflow release: branch előkészítés, aláírás, commit, finalizálás, tag, merge | Minden repo típus |
| `release-dependency` | Séma-specifikus: validator séma kiadása `dependencies/` könyvtárba | Séma repók |
| `release-schema` | Séma-specifikus: alkalmazás séma kiadása `release/` könyvtárba | Séma repók |
| `get-name` | A projekt manifest `metadata.name` értékének kiírása | Minden repo típus |

### Argumentum struktúra

```
compiler.py [--dry-run] [--verbose] [--debug]
            [--git-timeout N] [--vault-timeout N]
            <parancs> [parancs-argumentumok]

validate
    (nincs extra argumentum)

release
    --version X.Y.Z         kötelező

release-dependency
    --version X.Y.Z         kötelező

release-schema
    --version X.Y.Z         kötelező

get-name
    (nincs extra argumentum)
```

### Parancs-diszpécs logika a `compiler.py::main()`-ben

```python
manager = ReleaseManager(compiler_config, git_service, vault_service, ...)

match args.command:
    case "validate":
        manager.run_validation()

    case "release":
        manager.run_release_close(args.version)

    case "release-dependency":
        manager.run_release_dependency(args.version)

    case "release-schema":
        manager.run_release_schema(args.version)

    case "get-name":
        print(full_config["metadata"]["name"])
```

---

## 4. `project.yaml` struktúra — Végleges referencia

### 4.1 Mező-leltár

```yaml
# ── KÉZI mezők (emberek állítják be projekt inicializáláskor, a compiler nem érinti) ──
metadata:
  name: string                # ember-olvasható projekt neve
  description: string         # egymondat-összefoglaló
  version: string | null      # SemVer; null az első release előtt
  license: string             # SPDX azonosító (pl. CC-BY-NC-SA-4.0)
  main_branch: string         # Git main branch neve (pl. "main", "schemas")
  owner: string               # felelős csapat vagy egyén
  tags: [string]              # opcionális osztályozási tagek
  maintenance:                # opcionális
    status: active | maintenance-only | deprecated | end-of-life
    supported_until: YYYY-MM-DD
  contacts:                   # opcionális
    - type: email | slack | msteams
      value: string
  links:                      # opcionális
    - name: string
      url: string

# ── AUTO-GENERÁLT mezők (a compiler írja release során) ──
  validatedBy:                # melyik validator sémát használták
    name: string
    version: string
    checksum: string          # a validator séma spec blokkjának checksumja

  createdBy:                  # Vault tanúsítványból
    name: string              # CN a tanúsítványból
    email: string             # email a SubjectAltName-ből
    certificate: string       # teljes PEM (fejlesztői tanúsítvány)
    issuer_certificate: string  # teljes PEM (CIC Root CA)

  build_timestamp: string     # ISO-8601 UTC, release időpontjában kerül be

  validity:                   # opcionális, compiler_settings-ből tölti ki a compiler
    from: string              # ISO-8601
    until: string             # ISO-8601

  checksum: string            # kanonikus spec JSON SHA-256 hex digestje
  sign: string                # vault:v1:... fejlesztői aláírás

  buildHash: string           # a build lépés tölti ki (artifact checksum vagy git tree hash)
                              # üres string ("") amíg a build lépés le nem fut

  cicSign: string             # vault:v1:... CIC central authority aláírás
                              # üres string ("") amíg a finalize_release.py / relay le nem fut

  cicSignedCA:
    certificate: string       # CIC aláíró CA PEM tanúsítványa
                              # üres string ("") amíg a finalize_release.py / relay le nem fut

# ── KONFIGURÁCIÓS blokk (kézi, a compiler soha nem módosítja) ──
compiler_settings:
  component_name: string           # branch/tag elnevezésben használt
  main_branch: string              # merge-back célbranch
  canonical_source_file: string    # pl. "sources/index.yaml"
  meta_schema_file: string         # pl. "project.schema.yaml"
  meta_schemas_dir: string         # meta sémákat tartalmazó könyvtár
  source_dir: string               # séma forrásfájlok gyökere
  dependencies_dir: string         # pl. "dependencies/"
  release_dir: string              # pl. "release/"
  vault_key_name: string           # fejlesztői aláíró kulcs a Vault Transit-ban
  cic_root_ca_key_name: string     # CIC CA kulcs (finalize_release-hez)
  vault_cert_mount: string         # Vault KV mount tanúsítványokhoz
  vault_cert_secret_name: string   # secret neve fejlesztői tanúsítványhoz
  vault_cert_secret_key: string    # kulcs a KV secret-en belül
  cic_root_ca_secret_name: string  # secret neve CIC Root CA tanúsítványhoz
  validity_days: integer           # opcionális: hány napig érvényes egy release
```

### 4.2 Amit a `project.yaml` SOHA nem tartalmaz

- `spec` blokkot. A `spec` dedikált forrásfájlokban él (`sources/index.yaml`, workflow YAML-ok, stb.).
- Hardkódolt fájlútvonalakat, amelyek környezetenként eltérnek. Minden útvonal a `compiler_settings`-be kerül.

### 4.3 Az auto-generált mezők életciklusa

| Fázis | Írt mezők |
|---|---|
| Fejlesztői előkészítés (branch létrehozás) | `version`, `checksum`, `sign`, `build_timestamp`, `createdBy`, `validatedBy`, `validity`, `buildHash: ""`, `cicSign: ""`, `cicSignedCA.certificate: ""` |
| Build lépés (CI pipeline) | `buildHash` (tényleges artifact checksum vagy tree hash) |
| CIC finalizálás (finalize_release.py / relay) | `cicSign`, `cicSignedCA.certificate` |

---

## 5. Release flow repo típusonként

### 5.1 Séma repó (pl. `CIC-Schemas`)

```
Fejlesztő a main branchen
    │
    ├─ make validate
    │     └─ ReleaseManager.run_validation()
    │           ├─ schemalib/loader.load_and_resolve_schema(sources/index.yaml)
    │           ├─ schemalib/validator.get_validator_schema(dependencies/)  [+ integritás ellenőrzés]
    │           └─ schemalib/validator.run_validation(instance, validator_schema)
    │
    └─ make release-schema VERSION=1.0.0
          └─ ReleaseManager.run_release_schema("1.0.0")
                │
                ├─ 1. FÁZIS: Fejlesztői előkészítés (main-ről fut)
                │     ├─ git checkout -b schemas/releases/v1.0.0
                │     ├─ loader.load_and_resolve_schema(sources/index.yaml)
                │     ├─ validator.run_validation(spec, validator)
                │     ├─ artifact.compute_spec_checksum(spec)
                │     ├─ vault_service.get_certificate(developer_cert)
                │     ├─ vault_service.get_certificate(cic_root_ca_cert)
                │     ├─ artifact.parse_certificate_info(developer_cert) → name, email
                │     ├─ artifact.build_signing_payload(...) → digest_b64
                │     ├─ vault_service.sign(digest_b64, vault_key_name)
                │     ├─ artifact.generate_signed_artifact(...) → release_doc
                │     ├─ release/<name>-<version>.yaml fájl megírása
                │     ├─ git add + git commit "release: Prepare ..."
                │     └─ [ACTION REQUIRED] üzenet a fejlesztőnek
                │
                ├─ [Manuális vagy CI: build lépés — buildHash beírása project.yaml-ba]
                │
                └─ 2. FÁZIS: Finalizálás (release branchről fut)
                      ├─ project.yaml validálása project.schema.yaml ellen
                      ├─ git commit (ha dirty)
                      ├─ git tag schemas@v1.0.0
                      ├─ git checkout main
                      ├─ git merge --no-ff schemas/releases/v1.0.0
                      └─ git branch -d schemas/releases/v1.0.0

    [Merge után, külön lépésként:]
    └─ python -m tools.finalize_release project.yaml \
          --cic-vault-key cic-root-ca-key \
          --cic-cert-vault-path kv/data/secrets/CICRootCA:cert
          # Beírja: cicSign, cicSignedCA.certificate a project.yaml-ba
          # IDEIGLENES: a relay fogja ezt automatikusan elvégezni a jövőben
```

**A kétszintű release különbsége:**

- `release-dependency`: a kimenet a `dependencies/<name>-<version>.yaml` fájlba kerül. Ezek validator sémák, amelyeket más séma repók használnak.
- `release-schema`: a kimenet a `release/<name>-<version>.yaml` fájlba kerül. Ezek alkalmazás sémák, amelyeket szolgáltatások használnak.

A flow azonos; csak a kimeneti könyvtár és a `validatedBy` mező eltérő.

### 5.2 Workflow repó

```
Fejlesztő a main branchen
    │
    └─ make release VERSION=1.0.0
          └─ ReleaseManager.run_release_close("1.0.0")
                │
                ├─ 1. FÁZIS: Fejlesztői előkészítés
                │     ├─ git checkout -b <component>/releases/v1.0.0
                │     ├─ [Nincs spec betöltés — a workflow fájlok az igazság forrásai]
                │     ├─ workflow forrásfájlok checksumjának kiszámítása
                │     ├─ vault_service.get_certificate + sign
                │     ├─ project.yaml megírása (csak metadata, spec nélkül)
                │     └─ git add + git commit
                │
                └─ 2. FÁZIS: Finalizálás (séma repóval azonos)
```

Megjegyzés: workflow repók nem használják a `schemalib/validator.py`-t. A `run_validation()` metódus korán visszatér workflow repóknál (a `compiler_settings.repo_type: workflow` vezérli).

### 5.3 Modul / Relay repó

```
Fejlesztő a main branchen
    │
    └─ make release VERSION=1.0.0
          └─ ReleaseManager.run_release_close("1.0.0")
                │
                ├─ 1. FÁZIS: Fejlesztői előkészítés
                │     ├─ git checkout -b <component>/releases/v1.0.0
                │     ├─ git tree hash kiszámítása (git write-tree)
                │     ├─ vault_service.sign(tree_hash_digest)
                │     ├─ project.yaml megírása:
                │     │     metadata.checksum = tree_hash
                │     │     metadata.sign = vault_signature
                │     │     metadata.buildHash = ""   # Go build után töltendő ki
                │     └─ git add + git commit
                │
                ├─ [CI: go build → binary előállítása; binary checksum beírása buildHash-be]
                │
                └─ 2. FÁZIS: Finalizálás (git tag + merge)
```

Modul repók nem állítanak elő YAML artifactokat. A `buildHash` a lefordított Go binary SHA-256 checksumja (vagy több binary manifest-je).

---

## 6. `infra.py` refactor terve

### 6.1 Ami marad az `infra.py`-ban

- A `ReleaseManager` osztály és `__init__` aláírása (változatlan).
- Git workflow metódusok: `_check_base_branch_and_version`, `_execute_developer_preparation_phase`, `_execute_finalization_phase`, `run_release_close`.
- `_validate_final_project_yaml()` (project.yaml validálása project.schema.yaml ellen).
- `write_yaml()` segédmetódus (vagy áthelyezhető `schemalib/loader.py`-ba — mindkettő elfogadható).

### 6.2 Ami átkerül a `schemalib/`-ba

| Jelenlegi helye az `infra.py`-ban | Átkerül ide |
|---|---|
| `load_and_resolve_schema()` (egyszerű verzió) | `schemalib/loader.py` (felváltva a v18 robusztus verzióval) |
| `load_yaml()` | `schemalib/loader.py` |
| `_parse_certificate_info()` | `schemalib/artifact.py` |
| `to_canonical_json()` | `schemalib/artifact.py` |
| `get_sha256_hex()` | `schemalib/artifact.py` |
| inline aláírás payload felépítés (a `_execute_developer_preparation_phase`-ban) | `schemalib/artifact.build_signing_payload()` |
| inline artifact összeállítás (a `_execute_developer_preparation_phase`-ban) | `schemalib/artifact.generate_signed_artifact()` |
| `run_validation()` stub | `schemalib/validator.py` (teljes implementáció) |

### 6.3 A `ReleaseManager`-hez hozzáadott új metódusok

```python
def run_release_dependency(self, release_version: str) -> None:
    """
    Séma-specifikus release a dependencies/ könyvtárba.
    Delegál: _execute_schema_release(release_version, tier="dependency").
    """

def run_release_schema(self, release_version: str) -> None:
    """
    Séma-specifikus release a release/ könyvtárba.
    Delegál: _execute_schema_release(release_version, tier="application").
    """

def _execute_schema_release(self, release_version: str, tier: str) -> None:
    """
    Belső: validate → artifact generálás → fájl megírása.
    tier: "dependency" → dependencies/ könyvtár
    tier: "application" → release/ könyvtár
    """
```

### 6.4 Az új `run_validation()` implementáció

A jelenlegi `run_validation()` az `infra.py`-ban stub. A teljes implementáció:

```python
def run_validation(self) -> None:
    source_file = self._path(self.config.get("canonical_source_file", "sources/index.yaml"))
    source_data = schemalib.loader.load_and_resolve_schema(source_file)
    spec = source_data["spec"]

    validated_by = source_data.get("metadata", {}).get("validatedBy", {})
    validator_name = validated_by.get("name")
    validator_version = validated_by.get("version")
    expected_checksum = validated_by.get("checksum")

    if validator_name and validator_version:
        dependencies_dir = self._path(self.config.get("dependencies_dir", "dependencies"))
        validator_schema = schemalib.validator.get_validator_schema(
            validator_name, validator_version, dependencies_dir
        )
        schemalib.validator.verify_validator_integrity(validator_schema, expected_checksum)
        schemalib.validator.run_validation(spec, validator_schema)
    else:
        self.logger.warning("Nincs validatedBy konfigurálva — jsonschema validáció kihagyva.")
```

### 6.5 A `finalize_release.py` migrációs terve

A `finalize_release.py` egy **ideiglenes szkript**. Logikai felelősségei azonban permanensek:

1. `checksum == buildHash` ellenőrzése (build integritás kapu).
2. `cicSignedCA.certificate` beágyazása.
3. A végső dokumentum aláírása a CIC kulccsal (`cicSign`).
4. Visszaírás a `project.yaml`-ba.

**Migrációs terv:**
- Ha a relay üzembe helyezésre kerül, a fenti 1–4. lépések relay API hívássá válnak, amelyet a `ReleaseManager._execute_finalization_phase()` hív meg.
- A hívást a `compiler_settings.cic_relay_url` beállítottsága kapuazza.
- Ha nincs beállítva (lokális/dev mód), a `finalize_release.py` továbbra is manuálisan hívható.
- Ha a relay stabil és az összes repó migrált, a `finalize_release.py` törlendő.
- A `cicSign`/`cicSignedCA` mezők a `project.yaml`-ban permanens elemek maradnak.

---

## 7. Implementációs sorrend

Az implementáció a következő sorrendben haladjon a kockázat minimalizálása és a fokozatos tesztelés lehetővé tétele érdekében.

### 1. lépés — `schemalib/` váz létrehozása

Létrehozni a `tools/schemalib/__init__.py`-t üres exportokkal. Ez feloldja az import módosítások blokkolását a következő lépéseknél.

**Érintett fájlok:** `tools/schemalib/__init__.py`

### 2. lépés — `loader.py` portolása

`load_yaml`, `write_yaml` áthelyezése az `infra.py`-ból a `schemalib/loader.py`-ba.  
Az `infra.py`-ban lévő egyszerű `load_and_resolve_schema()` cseréje a v18 robusztus verzióra (beleértve a `convert_to_json_serializable()` + JSON körforgást).  
Az `infra.py` importjainak frissítése `schemalib.loader`-re.  
Az összes meglévő teszt frissítése az új import helyre.

**Érintett fájlok:** `tools/schemalib/loader.py`, `tools/infra.py`

### 3. lépés — `artifact.py` portolása

`_parse_certificate_info`, `to_canonical_json`, `get_sha256_hex` áthelyezése az `infra.py`-ból a `schemalib/artifact.py`-ba.  
`compute_spec_checksum()`, `build_signing_payload()`, `generate_signed_artifact()` hozzáadása.  
Az `infra.py` frissítése `schemalib.artifact` használatára.

**Érintett fájlok:** `tools/schemalib/artifact.py`, `tools/infra.py`

### 4. lépés — `validator.py` implementálása

`get_validator_schema()`, `verify_validator_integrity()`, `run_validation()` implementálása a `schemalib/validator.py`-ban.  
A `ReleaseManager`-ben lévő `run_validation()` stub felváltása a teljes implementációval, amely a `schemalib.validator`-t hívja.

**Érintett fájlok:** `tools/schemalib/validator.py`, `tools/infra.py`

### 5. lépés — `release-dependency` és `release-schema` parancsok hozzáadása

`run_release_dependency()`, `run_release_schema()`, `_execute_schema_release()` hozzáadása a `ReleaseManager`-hez.  
A két új subparser hozzáadása a `compiler.py`-hoz.  
A dispatch esetek hozzáadása a `main()`-ben.

**Érintett fájlok:** `tools/compiler.py`, `tools/infra.py`

### 6. lépés — `get-name` parancs hozzáadása

A `get-name` subparser és dispatch hozzáadása a `compiler.py::main()`-hez. Egy sor logika.

**Érintett fájlok:** `tools/compiler.py`

### 7. lépés — `repo_type` alapú routing hozzáadása

`compiler_settings.repo_type: schema | workflow | module` hozzáadása a `project.yaml`-hoz (és a `project.schema.yaml`-hoz).  
`run_validation()` és séma artifact generálás kapuzása `repo_type == "schema"` feltételre.

**Érintett fájlok:** `tools/infra.py`, `project.schema.yaml`

### 8. lépés — Tesztek írása a `schemalib/`-hoz

Tesztfájlok hozzáadása a `tests/test_tools/` alá minden új modulhoz:
- `test_schemalib_loader.py`
- `test_schemalib_validator.py`
- `test_schemalib_artifact.py`

Cél: minimum 85% lefedettség a `schemalib/`-on.

### 9. lépés — Dokumentáció frissítése

`docs/en/workflow.md` és `docs/hu/workflow.md` frissítése az új parancs-készlet tükrözéséhez.  
`docs/en/architecture.md` frissítése `schemalib/` hivatkozással.

### 10. lépés — `finalize_release.py` törlésre jelölése

Prominens `# DEPRECATED: Relay API elérhetővé válásakor használd.` kommentblokk hozzáadása.  
A relay készültségét külön mérföldkőként nyomon követni; törlés relay GA esetén.

---

## A Függelék: Kivétel hierarchia (változatlan)

```
ReleaseError (alap)
├── GitStateError
├── GitServiceError
├── VersionMismatchError
├── ConfigurationError
├── VaultServiceError
├── ManualInterventionRequired
└── ValidationFailureError   ← jelenleg infra.py-ban definiálva, kerüljön releaselib/exceptions.py-ba
```

## B Függelék: Környezeti változók

| Változó | Használja | Cél |
|---|---|---|
| `VAULT_ADDR` | `compiler.py` | Vault szerver URL |
| `VAULT_TOKEN` | `compiler.py` | Vault hitelesítési token |
| `VAULT_CACERT` | `compiler.py` | Vault CA tanúsítvány fájl útvonala |
| `CIC_VAULT_TOKEN_FILE` | `compiler.py` | Vault tokent tartalmazó fájl útvonala (alapértelmezett: `/var/run/secrets/vault-token`) |