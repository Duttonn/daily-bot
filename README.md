# Daily.fun M7 Calibrated Edge Sniper Trading Bot

Production-grade automated trading bot for **Daily.fun** 5-minute Bitcoin prediction markets (`btc-updown-5m`), powered by the **M7 Calibrated Edge Sniper Model** with dynamic Stop-Loss and capital preservation sizing.

---

## 1. Architecture & Strategy Overview

### The Quantitative Inefficiency
Daily.fun sets its orderbook option prices using a platform-assumed volatility formula and updates its market-maker quotes with a 5 to 15 second lag after rapid spot moves. In calm or moderate regimes, the platform frequently misprices the winning contract:
$$\sigma_{\text{house}} \approx 2.6 + 0.46 \times \sigma_{\text{market}}$$

### The M7 Calibrated Strategy
1. **Calibrated Sniping Window :** Continuously scans for mispriced contracts between **50s and 130s remaining** ($\tau \in [50, 130]$). The upper bound is strictly capped at 130s to prevent entering prematurely on 3.5-minute brownian noise.
2. **Effective Volatility & Conviction Filter ($Z \ge 0.70$) :**
   $$\sigma_{\text{eff}} = \max(\sigma_M, 2.20\ \$/\sqrt{s})$$
   $$Z = \frac{|d|}{\sigma_{\text{eff}} \sqrt{\tau}} \ge 0.70$$
   where $d = \text{BTC}_{\text{now}} - \text{BTC}_{\text{open}}$. The volatility floor ($2.20\ \$/\sqrt{s}$) prevents artificial inflation of $Z$ and $q$ on micro-tick noise.
3. **Probability & Net Edge Condition :**
   $$q = \Phi(Z)$$
   $$\text{Edge} = q - (\text{ask} + \text{fee}) \ge +10.0\% \quad (\text{with } \text{ask} \le 0.72\$)$$
4. **Hybrid Dynamic Stop-Loss / Cash Out :**
   Monitored between **30s and 90s remaining**. If the trade is compromised:
   $$q_{\text{surv}} = \Phi\left(\frac{d_{\text{rel}}}{\sigma_{\text{eff}} \sqrt{\tau}}\right) \le 0.30 \quad \text{OR} \quad d_{\text{rel}} \le -5.0\$$$
   The bot automatically triggers a taker sell at best bid into the Daily.fun book, cutting losses to preserve capital rather than taking a -100% loss at settlement.
5. **Execution :** Signs on-chain Solana taker intent (v4) with root Cash authorization and routes orders via Daily.fun WebSocket in $<70\text{ ms}$.

---

## 2. Capital Preservation & Sizing

- **Allocation per trade :** `5%` of live USDC balance (aligned with conservative half-Kelly allocation).
- **Leverage Multiplier :** Fixed at `1.0x` (dynamic multipliers disabled to protect the vault against asymmetric drawdowns).
- **Drawdown Protection :** Dynamic balance floor preserving at least `50%` of initial capital.
- **Minimum notional :** Strictly respects Daily.fun $1.00 minimum notional requirement.
- **Depth Absorption :** Inspects Level 1 and Level 2 asks to absorb available book depth without exceeding slippage bounds.

### Capital Optimization & Liquidity Ceiling
| Capital actif | Mise par trade (12%) | Parts achetées | Remplissage carnet |
|---|---|---|---|
| **50 $** | ~6.00 $ | ~8-9 parts | Absorption mineure |
| **100 $** | ~12.00 $ | ~16-18 parts | Niveau 1 partiel |
| **650 $** | ~78.00 $ | ~110-115 parts | **Sature 100% du Niveau 1** |
| **3 500 $** | ~400.00 $ | ~550-575 parts | **Sature Niveau 1 + Niveau 2** |

> **Plafond optimal :** Entre **650 $ et 800 $**, le bot sature l'intégralité du Niveau 1 à la meilleure cote. Entre **3 000 $ et 3 500 $**, il absorbe les Niveaux 1 et 2 (~400 $ de mise par manche). Au-delà de 3 500 $, le carnet Daily.fun manque de liquidité immédiate et les gains journaliers absolus plafonnent.

---

## 3. Guide de Déploiement : VPS Canada (AWS EC2)

Pour minimiser la latence réseau avec Solana et les services Daily.fun, l'hébergement recommandé est **AWS ca-central-1 (Montréal, Canada)** ou **us-east-1 (Ashburn, Virginie)**.

### Étape 1 : Créer l'instance EC2
1. Rendez-vous sur la console **AWS EC2** en région `ca-central-1` (Canada Central).
2. Cliquez sur **Launch Instance** :
   - **Nom :** `daily-bot-vps`
   - **OS :** `Ubuntu Server 24.04 LTS (HVM), SSD Volume Type (64-bit x86)`
   - **Type d'instance :** `t3.small` (2 vCPU, 2 GiB RAM) ou `t3.micro`
   - **Key pair :** Créez ou sélectionnez votre clé SSH (`.pem`)
   - **Security Group :** Autoriser uniquement le port `22` (SSH) depuis votre IP
   - **Stockage :** 20 GiB gp3
3. Lancez l'instance et notez son IP publique (ex: `15.156.83.71`).

### Étape 2 : Connexion SSH et Préparation du Système
Connectez-vous depuis votre terminal local :
```bash
ssh -i /chemin/vers/votre-cle.pem ubuntu@<IP_VPS>
```

Installez Node.js 20 LTS, git et tmux :
```bash
# Mise à jour système
sudo apt update && sudo apt upgrade -y

# Installation Node.js 20 LTS
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt install -y nodejs git tmux htop

# Vérification des versions
node -v   # v20.x.x
npm -v    # 10.x.x
```

### Étape 3 : Installation du Bot
Clonez le dépôt sur le VPS :
```bash
git clone git@github.com:Duttonn/daily-bot.git ~/daily-bot
cd ~/daily-bot

# Installation des dépendances légères (web3.js, tweetnacl, ws)
npm install
```

---

## 4. Configuration des Clés & Authentification (`.env`)

Copiez le modèle d'environnement :
```bash
cp .env.example .env
chmod 600 .env
nano .env
```

Renseignez les variables :
```env
# Adresse publique de votre wallet Solana (Owner)
OWNER_ADDRESS=VOTRE_ADRESSE_SOLANA_ICI

# Clé privée de la Session Authority Daily.fun (tableau de 64 entiers)
SESSION_SECRET_KEY=[12,34,56,...]

# Tokens Privy (récupérés depuis Daily.fun)
PRIVY_TOKEN=eyJhbGciOi...
PRIVY_REFRESH_TOKEN=eyJhbGciOi...

# Mode Paper (true pour simulation sans argent réel, false pour live)
PAPER_MODE=false
```

### Comment extraire vos clés depuis Daily.fun ?

#### Prérequis Obligatoire : Activation du Cash Vault via un Premier Trade Manuel (1 $)
Avant de pouvoir extraire les clés pour le VPS, le compte Daily.fun **doit être initialisé on-chain** :
1. Connectez-vous sur [https://daily.fun](https://daily.fun) avec votre wallet Solana (Phantom, Solflare, etc.).
2. Déposez vos USDC sur la plateforme.
3. **Placez obligatoirement au moins un premier trade manuel de 1.00 $** directement sur le site :
   - Cette action déclenche la signature d'activation du vault (`daily_cash_vault_live`) et crée la session déléguée 1-click on-chain (`trading_session_v4`).
   - Sans ce premier trade manuel, le smart contract Solana n'a pas enregistré le vault, le compte reste bloqué pour les ordres API (`betting_blocked`), et la clé privée de session n'existe pas encore dans le stockage local.
4. Une fois le trade de 1 $ confirmé à l'écran, vous pouvez extraire vos clés ci-dessous.

#### A. Obtenir les Tokens Privy pour l'Auto-Rotation 24/7
Le jeton de session Daily.fun (`PRIVY_TOKEN`) expire toutes les 60 minutes. Pour permettre au bot de tourner sans interruption pendant des semaines, il a impérativement besoin du `PRIVY_REFRESH_TOKEN`.
Toutes les 20 minutes (et automatiquement en cas d'erreur `bad token`), le bot contacte l'API Privy (`https://auth.privy.io/api/v1/sessions`) pour renouveler son JWT et sauvegarde le nouveau token à chaud.

**Méthode 1-clic depuis la console du navigateur :**
1. Connectez-vous sur [https://daily.fun](https://daily.fun) avec votre wallet Solana.
2. Ouvrez les **Outils de développement** (`F12` ou `Cmd + Option + I` sur Mac).
3. Cliquez sur l'onglet **Console**.
4. Collez la commande suivante et appuyez sur **Entrée** :
   ```javascript
   copy(`PRIVY_TOKEN="${localStorage.getItem("privy:token")}"\nPRIVY_REFRESH_TOKEN="${localStorage.getItem("privy:refresh_token")}"`);
   console.log("✅ Tokens copiés dans le presse-papiers !");
   ```
5. Ouvrez votre fichier `.env` sur le VPS et collez directement (`Ctrl+V` ou `Cmd+V`).

**Méthode manuelle (Storage) :**
- Dans les DevTools, allez sur l'onglet **Application** (ou **Stockage**) > **Local Storage** > `https://daily.fun`.
- Cherchez la clé `privy:token` : double-cliquez sur sa valeur pour la copier (`PRIVY_TOKEN`).
- Cherchez la clé `privy:refresh_token` : copiez sa valeur (`PRIVY_REFRESH_TOKEN`).

#### B. Obtenir l'adresse de Wallet (`OWNER_ADDRESS`)
C'est l'adresse publique Solana de votre wallet Phantom / Backpack / Solflare connecté à Daily.fun (ex: `7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU`), visible en cliquant sur votre profil en haut à droite.

#### C. Obtenir la Clé de Session (`SESSION_SECRET_KEY`)
Daily.fun utilise un mécanisme de session authority déléguée (`trading_session_v4`) sur Solana pour signer les ordres sans ouvrir une popup de wallet à chaque trade de 5 minutes.
Cette clé privée de 64 octets se présente sous la forme d'un tableau JSON d'entiers :
```env
SESSION_SECRET_KEY=[145,22,89,12,4,233,18,...]
```
Elle est générée lors de l'activation initiale de la session de trading sur la plateforme (trading 1-click suite à votre premier trade manuel de 1 $).

**Extraction 1-clic depuis la console du navigateur :**
1. Sur Daily.fun, ouvrez la console DevTools (`F12` ou `Cmd + Option + I`).
2. Collez la commande suivante et appuyez sur **Entrée** :
   ```javascript
   const req = indexedDB.open("daily-markets-device-capabilities", 1);
   req.onsuccess = () => {
     const tx = req.result.transaction("taker-v4", "readonly");
     const getReq = tx.objectStore("taker-v4").getAll();
     getReq.onsuccess = () => {
       const key = getReq.result[0]?.secretKey;
       if (key) {
         console.log("SESSION_SECRET_KEY=" + JSON.stringify(Array.from(key)));
       } else {
         console.error("Aucune clé trouvée. Avez-vous bien passé un premier trade de 1 $ ?");
       }
     };
   };
   ```
3. Copiez la ligne `SESSION_SECRET_KEY=[...]` affichée dans la console et collez-la dans votre fichier `.env` sur le VPS.

> **IMPORTANT - Cycle de vie et contrainte d'unicité de la session :**
> - **Validité :** Une session déléguée est valide pour **7 jours pleins** (168 heures) sur la blockchain Solana. Le bot surveille l'expiration on-chain et recharge les tokens automatiquement.
> - **Unicité on-chain :** Le smart contract Solana de Daily.fun n'autorise qu'**une seule session active par compte à la fois**. Si vous activez le mode 1-click ou passez un trade depuis un autre appareil (smartphone, autre navigateur), une nouvelle clé est générée et la session du VPS est **immédiatement révoquée on-chain** (`[SESSION EXPIRED]`).
> - **Règle d'exploitation :** Ne jamais activer le mode 1-click ou trader manuellement depuis votre téléphone pendant que le bot tourne sur le VPS. Vous pouvez consulter le site ou votre solde en lecture seule sans risque.

---

## 5. Démarrage en Production (tmux)

Lancez le bot dans une session `tmux` persistante :
```bash
tmux new-session -d -s daily-bot -c ~/daily-bot 'node daily-s6-bot.mjs 2>&1 | tee -a bot.log'
```

### Commandes utiles de gestion
```bash
# Voir les logs en direct
tmux attach -t daily-bot

# Se détacher sans arrêter le bot
# Appuyez sur Ctrl+B puis D

# Consulter les 50 dernières lignes de log
tail -n 50 ~/daily-bot/bot.log

# Vérifier le statut des processus
ps aux | grep daily-s6-bot
```

---

## 6. Dashboard Temps Réel (Euphoria Style)

Un dashboard autonome est fourni dans `dashboard/` :
- Courbe de solde adaptative en SVG.
- P&L par manche avec histogramme centré.
- Chronologie 5 minutes avec repère interactif de la fenêtre F20.
- Suivi du protocole de validation des 300 trades ($Z$-score $\ge 2\sigma$).

Pour lancer le serveur de dashboard en local :
```bash
node dashboard/updown-server.mjs
```
Ouvrez votre navigateur sur `http://localhost:8789`.

---

## 7. Sécurité & Bonnes Pratiques

- **Jamais de clés dans Git :** `.env` et `auth.json` sont exclus par `.gitignore`.
- **Circuit Breaker :** Arrêt automatique des ordres en cas de perte de session on-chain ou de rupture du plancher de solde.
- **Auto-Rotation Privy :** Le bot rafraîchit automatiquement son jeton JWT Privy toutes les 20 minutes pour éviter toute déconnexion intempestive.

---

## Licence
MIT License.
