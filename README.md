# Daily.fun S6 Up/Down Trading Bot

Production-grade automated trading bot for **Daily.fun** 5-minute Bitcoin prediction markets (`btc-updown-5m`), powered by the **S6 Forward Edge Model** (F20 timing window).

---

## 1. Architecture & Strategy Overview

### The Quantitative Inefficiency (F19 Mechanism)
Daily.fun sets its orderbook option prices using a platform-assumed volatility formula:
$$\sigma_{\text{house}} \approx 2.6 + 0.46 \times \sigma_{\text{market}}$$

When the Bitcoin market enters a calm regime ($\sigma_M < 2.30\ \$/\sqrt{s}$), the platform expects $\sigma_{\text{house}} \approx 3.7 - 3.9\ \$/\sqrt{s}$. As a result, **Daily.fun significantly underprices the leading outcome 1 to 3 minutes before the round expires**.

### The S6 Model (F20 Window)
1. **Timing Window :** Evaluates exactly at **2 minutes before round close** ($\tau = 118\text{s} - 122\text{s}$ remaining).
2. **Realized Volatility Filter :** Samples Binance Futures BTCUSDT tick prices on a 500 ms grid over the last 10 minutes. Only trades if $\sigma_M < 2.30\ \$/\sqrt{s}$.
3. **Probability Calculation :**
   $$q = \Phi\left(\frac{|d|}{\sigma_M \sqrt{\tau}}\right)$$
   where $d = \text{BTC}_{\text{now}} - \text{BTC}_{\text{open}}$.
4. **Edge Condition :** Buys when net edge exceeds 5 cents:
   $$q - (\text{ask} + \text{fee}) \ge +0.05$$
5. **Execution :** Signs on-chain Solana taker intent (v4) with root Cash authorization and routes order via Daily.fun matching engine WebSocket in $<70\text{ ms}$.

---

## 2. Dynamic Capital Sizing

The bot automatically adapts to **any starting capital** deposited on Daily.fun:
- **Allocation per trade :** `12%` of active live USDC balance.
- **Drawdown Protection :** Dynamic balance floor preserving at least `50%` of initial capital (stop-loss).
- **Minimum notional :** Strictly respects Daily.fun $1.00 minimum notional requirement.
- **Depth Absorption :** Dynamically inspects Level 1 and Level 2 asks to absorb available book depth without exceeding slippage bounds.

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
1. Connectez votre wallet sur [Daily.fun](https://daily.fun).
2. Ouvrez les **Outils de développement Chrome** (`F12` ou `Cmd+Option+I`) > Onglet **Application** > **Local Storage** > `https://daily.fun`.
3. Récupérez :
   - `privy:token` -> `PRIVY_TOKEN`
   - `privy:refresh_token` -> `PRIVY_REFRESH_TOKEN`
4. La session Authority Solana est créée lors de votre première approbation de session de trading sur Daily.fun (sauvegardée localement par Privy).

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
