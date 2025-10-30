# Setting Up GitHub SSH Key

## Which SSH Key to Use

You have multiple SSH keys. Here's which one to add to GitHub:

### **Recommended: Ed25519 Key** (Modern & Secure)
This is the key you should add to GitHub:
```
~/.ssh/id_ed25519.pub
```

### Alternative: RSA Key
If Ed25519 doesn't work, you can use:
```
~/.ssh/id_rsa.pub
```

## Steps to Add SSH Key to GitHub

1. **Copy your Ed25519 public key**:
   ```bash
   cat ~/.ssh/id_ed25519.pub | pbcopy
   ```
   (This copies it to your clipboard on Mac)

2. **Add it to GitHub**:
   - Go to: https://github.com/settings/keys
   - Click "New SSH key"
   - Title: "MacBook Pro Airflow" (or any name you prefer)
   - Paste the key from clipboard
   - Click "Add SSH key"

3. **Test the connection**:
   ```bash
   ssh -T git@github.com
   ```
   You should see: "Hi OrestOhorodnyk! You've successfully authenticated..."

4. **Update your git remote to use SSH** (if using HTTPS):
   ```bash
   cd "/Users/oohor/Documents/airflow on k8s/dbt_repo_aggregator"
   git remote set-url origin git@github.com:OrestOhorodnyk/dbt_repo_aggregator.git
   ```

5. **Push your changes**:
   ```bash
   git push origin dev
   ```

## Display Your Ed25519 Key

To see your Ed25519 key to copy:
```bash
cat ~/.ssh/id_ed25519.pub
```

Or to copy it directly:
```bash
cat ~/.ssh/id_ed25519.pub | pbcopy
```

