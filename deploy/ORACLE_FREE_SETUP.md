# Running the recorder for $0 on Oracle Cloud "Always Free"

This sets up one always-on ARM VM that records Kalshi data around the clock. Optionally,
it also copies the most recent 14 days to free off-site storage. There is no backup host,
so if the VM is down that data is lost, and the gap is logged in the `events` stream.

> Free-tier terms change. Check Oracle's current Always Free limits and idle-instance policy
> when you sign up (see step 9).

## 1. Create the account and VM

1. Sign up at oracle.com/cloud/free. A card is required for identity checks only; Always
   Free resources are not charged.
2. **Home region: choose `US East (Ashburn)`.** Always Free resources exist only in your
   home region, and the home region can't be changed later. Ashburn is close to most US
   exchange infrastructure.
3. Create the instance under Compute → Instances → Create:
   - Image: **Ubuntu 24.04** (aarch64)
   - Shape: **VM.Standard.A1.Flex**, **2 OCPU / 12 GB RAM**. The free allowance is
     4 OCPU / 24 GB in total, which leaves room for the model process later.
   - Boot volume: **200 GB**. That is the whole free block-storage allowance and holds
     about a year of recordings.
   - Add your SSH public key.
   - If you get "Out of capacity", try another availability domain or retry later. This
     is common on the free tier.
4. Note the public IP. The recorder only makes outbound connections, so no inbound ports
   are needed beyond SSH.

## 2. Base packages and clock

```bash
ssh ubuntu@<public-ip>
sudo apt update && sudo apt -y upgrade
sudo apt -y install python3-venv git chrony rclone
sudo timedatectl set-timezone UTC
chronyc tracking            # "System time" offset should be a few ms at most
```

## 3. User, folders, code

```bash
sudo useradd --system --create-home --home-dir /opt/kalshi-trading --shell /usr/sbin/nologin kalshi
sudo mkdir -p /var/lib/kalshi/raw /etc/kalshi
sudo chown -R kalshi:kalshi /var/lib/kalshi
sudo chmod 750 /etc/kalshi && sudo chown root:kalshi /etc/kalshi
```

The repository is private, so give the VM a **read-only deploy key**:

```bash
sudo -u kalshi mkdir -m 700 -p /opt/kalshi-trading/.ssh
sudo -u kalshi ssh-keygen -t ed25519 -N "" -f /opt/kalshi-trading/.ssh/id_ed25519
sudo cat /opt/kalshi-trading/.ssh/id_ed25519.pub
# GitHub → repo → Settings → Deploy keys → Add key (leave "write access" unchecked)
sudo -u kalshi git clone git@github.com:vladk-III/Kalshi-Trading.git /opt/kalshi-trading/src
```

Then install into a virtual environment. The systemd unit expects the code at
`/opt/kalshi-trading`, so the clone and the venv are linked like this:

```bash
cd /opt/kalshi-trading
sudo -u kalshi bash -c 'shopt -s dotglob; mv src/* . && rmdir src'
sudo -u kalshi python3 -m venv .venv
sudo -u kalshi .venv/bin/pip install .
```

## 4. Kalshi API key

1. On kalshi.com, go to Account → API keys and create a key **used only for recording**
   (trading will get its own key later). Download the private key file.
2. Copy it to the VM and lock it down:

```bash
scp kalshi-recorder.key ubuntu@<public-ip>:/tmp/
sudo mv /tmp/kalshi-recorder.key /etc/kalshi/recorder.pem
sudo chown kalshi:kalshi /etc/kalshi/recorder.pem && sudo chmod 600 /etc/kalshi/recorder.pem
sudo cp /opt/kalshi-trading/deploy/kalshi-recorder.env.example /etc/kalshi/recorder.env
sudo nano /etc/kalshi/recorder.env      # set KALSHI_API_KEY_ID
sudo chown kalshi:kalshi /etc/kalshi/recorder.env && sudo chmod 600 /etc/kalshi/recorder.env
```

## 5. First run in the foreground

```bash
cd /opt/kalshi-trading
sudo -u kalshi bash -c 'set -a; . /etc/kalshi/recorder.env; set +a; \
  .venv/bin/kalshi-recorder --config configs/recorder.yaml'
```

Within a minute you should see `connected`, `subscriptions` (added = the current 15M
windows and ladder strikes) and, after 60 s, a `stats` line. Press Ctrl-C to stop cleanly.

Things to check (they answer several **[verify]** items in the design doc):

```bash
F=$(ls /var/lib/kalshi/raw/kalshi_index/*/*.zst | tail -1)
.venv/bin/kalshi-read-raw "$F" --type cfbenchmarks_value | head -3   # official index ticks
.venv/bin/kalshi-read-raw /var/lib/kalshi/raw/events/*/*.zst | grep -E 'subscribe_failed|ws_error'
.venv/bin/kalshi-read-raw /var/lib/kalshi/raw/market_meta/*/*.zst | head -1   # strike fields
```

- `subscribe_failed` for `cfbenchmarks_value` means the account lacks access to the
  index feed. Contact Kalshi support before going further, because the design depends on it.
- Series that never appear in `subscriptions` (e.g. `KXETHD`) have the wrong ticker. Fix
  them in `configs/recorder.yaml`.

## 6. Run it as a service

```bash
sudo cp deploy/systemd/kalshi-recorder.service deploy/systemd/kalshi-retention.* /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now kalshi-recorder kalshi-retention.timer
journalctl -u kalshi-recorder -f
```

systemd restarts the recorder within 5 s if it ever exits, and starts it again on reboot.

## 7. Free phone alerts (optional, recommended)

Install the **ntfy** app, subscribe to a hard-to-guess topic, and set
`ntfy_url: https://ntfy.sh/<your-topic>` in `configs/recorder.yaml`. You'll get a push
notification on disconnects, a stale index feed, low disk space or crashes, at most one per
kind every 5 minutes.

## 8. Free off-site copy of recent data (optional)

Cloudflare R2 and Backblaze B2 each give 10 GB free, which fits about 14 days of
recordings. This protects recent data if the VM is lost; older data lives only on the VM.

```bash
sudo -u kalshi rclone config --config /etc/kalshi/rclone.conf   # create a remote named "offsite"
sudo cp deploy/systemd/kalshi-backup.* /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now kalshi-backup.timer
```

## 9. Idle-instance reclamation (important)

Oracle may **reclaim Always Free VMs it considers idle** (low CPU, network and memory use
over 7 days). A recorder on its own uses very little CPU, so it could qualify. Check the
current policy. Your options:

- Upgrade the account to **Pay As You Go**. Always Free resources stay free, and the idle
  reclamation rule doesn't apply to paid accounts. **Set a budget alert of $1** under
  Billing → Budgets so any accidental paid resource is caught right away.
- Or accept the risk until the model/strategy processes run on the same VM, which raises
  utilisation naturally.

Don't run fake CPU load to dodge the rule.

## 10. Updating

```bash
cd /opt/kalshi-trading
sudo -u kalshi git pull
sudo -u kalshi .venv/bin/pip install .
sudo systemctl restart kalshi-recorder      # files close cleanly; the gap is ~5 s and is logged
```
