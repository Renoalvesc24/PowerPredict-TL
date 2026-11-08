# PowerPredict TL – Oracle Cloud Always Free setup (your part)

Takes about 20–30 minutes. Only you can do these steps (your account, your card). When you finish, send Claude the **Public IP** (and the DuckDNS name from step 5).

## 1. Create the free account
1. Open **https://signup.cloud.oracle.com** → *Sign up for Oracle Cloud Free Tier*.
2. **Home region:** pick one close to Timor-Leste, e.g. **Australia East (Sydney)**, **Singapore** or **Japan East (Tokyo)**. It can't be changed later.
3. Verify with your bank card. Oracle uses it only to check identity; Always Free resources are not charged.
4. Stay on the free plan (don't click "Upgrade to Pay As You Go").

## 2. Create the server
Menu ☰ → **Compute → Instances → Create instance**

| Setting | Value |
|---|---|
| Name | `powerpredict-tl` |
| Image | *Change image* → **Canonical Ubuntu 24.04** |
| Shape | *Change shape* → **Ampere** → **VM.Standard.A1.Flex** → **2 OCPU, 12 GB memory** (stays inside Always Free) |
| Networking | *Create new virtual cloud network* + *public subnet*, **Assign a public IPv4 address: Yes** |
| SSH keys | **Paste public keys** → paste the line below |
| Boot volume | *Specify a custom size* → **100 GB** |

SSH public key to paste (one line):
```
ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFJYu2CxYwpPS2wQNre1jiGRFqKva7S3WBw77ph0fk7V powerpredict-oracle
```

Click **Create**. If you see **"Out of capacity"**, choose another *Availability Domain* and try again, or retry a few hours later. This is common.

## 3. Open the website ports
On the instance page: **Primary VNIC → Subnet → Security Lists → Default Security List → Add Ingress Rules**

| Source CIDR | IP Protocol | Destination port |
|---|---|---|
| `0.0.0.0/0` | TCP | `80` |
| `0.0.0.0/0` | TCP | `443` |

(Port 22 for SSH is already open by default.)

## 4. Copy the Public IP
On the instance page, copy **Public IPv4 address** (looks like `152.67.x.x`).

## 5. Free web address with HTTPS (recommended)
1. Open **https://www.duckdns.org** → sign in with **GitHub**.
2. Add a sub domain, e.g. `powerpredict-tl` → **powerpredict-tl.duckdns.org**.
3. Put your server's Public IP in *current ip* → **update ip**.

## 6. Send Claude
- the **Public IP**
- the **DuckDNS name** (e.g. `powerpredict-tl.duckdns.org`)

Claude does the rest over SSH: installs Docker, uploads the system, starts Kafka, Spark, MySQL, the API, the bot and the website, and sets up HTTPS.
