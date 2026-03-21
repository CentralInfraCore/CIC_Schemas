# Fejlesztői Munkafolyamat

Ez a dokumentum a séma keretrendszerrel való interakció tipikus munkafolyamatait vázolja fel, az első beállítástól egy új kiadás létrehozásáig.

## Első Beállítás

Mielőtt elkezdenéd, győződj meg róla, hogy a következő előfeltételek telepítve vannak a gépeden:
- `docker`
- `docker-compose`
- `make`
- `git`

Kövesd ezeket a lépéseket a projekt inicializálásához a repository klónozása után:

1.  **A Vault Aláíró Ügynök Elindítása:**
    A projekthez szükség van egy futó Vault példányra a kiadási artefaktumok aláírásához. Egy segédszkript biztosított egy ideiglenes, helyi Vault szerver futtatásához fejlesztés céljából.

    ```sh
    # Ezt a projekt gyökeréből kell futtatni egy külön terminálban
    ./tools/vault-sign-agent.sh -k /eleresi/ut/a/kulcsodhoz.pem -c /eleresi/ut/a/certedhez.crt --root-ca-file /eleresi/ut/a/CICRootCA.crt
    ```
    Ez az ügynök a háttérben fog futni.

2.  **Python Függőségek Telepítése:**
    Ez a parancs lefordítja a `requirements.in` fájlt, és telepíti az összes szükséges Python csomagot egy helyi `./p_venv` könyvtárba, amelyet a Docker konténer gyorsítótárként használ.

    ```sh
    make infra.deps
    ```

3.  **Docker Image-ek Építése:**
    Építsd meg a `setup` és `builder` szolgáltatásokhoz szükséges Docker image-eket.

    ```sh
    make build
    ```

4.  **A Fejlesztői Konténer Elindítása:**
    Ez elindítja a `builder` konténert a háttérben.

    ```sh
    make up
    ```

5.  **Git Hook-ok Inicializálása:**
    Ez a szkript beállítja a `commit-msg` Git hookot, amely automatikusan aláírja a commitjaidat a futó Vault ügynök segítségével.

    ```sh
    make repo.init
    ```

A környezeted most már teljesen be van állítva és készen áll a fejlesztésre.

## Fordítói Parancsok Referenciája

A fordító (`python -m tools.compiler`) a központi eszköz. Az elérhető parancsok a `project.yaml`-ban beállított `compiler_settings.repo_type` értékétől függnek.

| Parancs | Repo típus | Leírás |
|---|---|---|
| `validate` | `schema` | Forrás séma offline validálása a deklarált validátor ellen (integritás ellenőrzéssel). |
| `release --version X.Y.Z` | minden | Teljes Git-munkafolyamat kiadás: ág, aláírás, commit, tag, összefűzés. |
| `release-dependency --version X.Y.Z` | `schema` | Validátor/meta séma kiadása a `dependencies/` könyvtárba. |
| `release-schema --version X.Y.Z` | `schema` | Alkalmazás séma kiadása a `release/` könyvtárba. |
| `get-name` | minden | A `metadata.name` kiírása a `project.yaml`-ból. |

Globális jelzők, amelyek minden parancsnál elérhetők:

```
--dry-run         Minden művelet szimulálása írás vagy commit nélkül.
--verbose / -v    INFO szintű naplóüzenetek megjelenítése.
--debug   / -d    DEBUG szintű naplóüzenetek megjelenítése (legrészletesebb).
--git-timeout N   Git alfolyamat időkorlátja másodpercekben (alapértelmezett: 60).
--vault-timeout N Vault API időkorlátja másodpercekben (alapértelmezett: 10).
```

## Napi Fejlesztési Feladatok

Ez a tipikus ciklus, amelyet a sémák módosításakor vagy létrehozásakor követni fogsz.

1.  **Séma Módosítása:**
    Végezd el a kívánt módosításokat egy sémafájlon a `sources/` könyvtárban.

2.  **Validálás Futtatása (csak schema repo):**
    Mielőtt kiadást hoznál létre, validáld a módosításaidat. A `validate` parancs betölti a forrás sémát, feloldja az összes `$ref` hivatkozást, ellenőrzi a validátor integritását, és futtatja a jsonschema validálást.

    ```sh
    make validate
    # vagy részletes kimenettel:
    make validate VERBOSE=1
    ```

3.  **Tesztek Futtatása:**
    Annak érdekében, hogy maguk az eszközök is megfelelően működjenek, futtasd a `pytest` tesztcsomagot.

    ```sh
    make test
    ```

4.  **Módosítások Commit-olása:**
    Amikor készen vagy, commit-old a módosításaidat. A `commit-msg` hook automatikusan lefut, csatlakozik a helyi Vault ügynökhöz, és egy aláírási blokkot fűz a commit üzenetedhez.

    ```sh
    git add .
    git commit -m "feat: Séma frissítése új tulajdonságokkal"
    ```

## Kiadás Létrehozása

### Séma Repository-k

A séma repo-k két kiadási parancsot támogatnak attól függően, hogy milyen típusú artefaktum keletkezik:

- **`release-dependency`** — aláírt validátor sémát állít elő a `dependencies/` könyvtárba. Más repo-k által felhasznált meta-sémákhoz használatos.
- **`release-schema`** — aláírt alkalmazás sémát állít elő a `release/` könyvtárba. Szolgáltatások által felhasznált sémákhoz használatos.

```sh
# Validátor séma kiadása (pl. template-schema)
make release-dependency VERSION=v1.0.0

# Alkalmazás séma kiadása (pl. postgresql)
make release-schema VERSION=v1.0.0
```

A fordító a következőket hajtja végre:
1. Betölti és validálja a forrás sémát a `sources/index.yaml`-ból.
2. Ellenőrzi a validátor séma integritását (ellenőrzőösszeg).
3. SHA-256 ellenőrzőösszeget számol a `spec` blokkhoz.
4. Lekéri az aláíró tanúsítványt és a CIC Root CA-t a Vault-ból.
5. Aláírja az artefaktum metaadatait a Vault kulcsoddal.
6. Kiírja az aláírt artefaktumot a `dependencies/` vagy `release/` könyvtárba.

### Minden Repository Típus (Git munkafolyamat kiadás)

A `release` parancs a teljes Git munkafolyamatot futtatja: kiadási ágat hoz létre, aláírja és commit-olja a `project.yaml`-t, majd megvárja a build lépést a véglegesítés előtt.

**1. fázis — Fejlesztői előkészítés (futtatás a main ágról):**

```sh
make release VERSION=1.0.0
```

Ez létrehoz egy kiadási ágat (pl. `base/releases/v1.0.0`), aláírja a projekt metaadatait, és commit-olja a `project.yaml`-t. Ezután arra kér, hogy futtasd a build folyamatot.

**2. fázis — Véglegesítés (futtatás a kiadási ágról):**

Miután a build lépés frissítette a `buildHash`-t a `project.yaml`-ban:

```sh
make release VERSION=1.0.0
```

A fordító felismeri a kiadási ágat és végrehajtja a véglegesítést: validálja a `project.yaml`-t, annotált taget hoz létre, visszafűzi a main-ba, és törli a kiadási ágat.

**CIC Központi Aláírás (opcionális utólépés):**

A véglegesítés után a CIC hatóság alkalmazhat egy második aláírást:

```sh
python -m tools.finalize_release project.yaml \
  --cic-vault-key cic-root-ca-key \
  --cic-cert-vault-path kv/data/secrets/CICRootCA:cert
```

Ez kitölti a `cicSign` és `cicSignedCA.certificate` mezőket a `project.yaml`-ban.

> **Megjegyzés:** A `finalize_release.py` egy ideiglenes szkript. Ezt a lépést a relay infrastruktúra fogja automatizálni egy jövőbeli kiadásban.

### Száraz Futtatás Módja

Minden kiadási parancs támogatja a `--dry-run` jelzőt (vagy a `DRY_RUN=1` értéket a make-en keresztül). Nem kerül sor fájlírásra, Git műveletre, és a Vault hívások helyőrző értékeket adnak vissza.

```sh
make release VERSION=1.0.0 DRY_RUN=1
make release-schema VERSION=v1.0.0 DRY_RUN=1
```
