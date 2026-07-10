#!/usr/bin/env bash

set -Eeuo pipefail

readonly PUBLIC_IP="8.148.66.77"
readonly LIVEKIT_IMAGE="livekit/livekit-server:v1.13.1"
readonly NGINX_IMAGE="nginx:1.27.5-alpine"
readonly CERTBOT_IMAGE="certbot/certbot:v5.4.0"
readonly LIVEKIT_DIR="/data/app/livekit"
readonly CERTBOT_DIR="/data/app/certbot"
readonly CERTBOT_WEBROOT="${CERTBOT_DIR}/www"
readonly CERT_PATH="${CERTBOT_DIR}/live/${PUBLIC_IP}"
readonly CHAT_CONFIG="/data/app/chat/config/chat-rpc-chat.yml"
readonly CHAT_CONFIG_ROLLBACK="${LIVEKIT_DIR}/chat-rpc-chat.yml.rollback"
readonly CREDENTIALS="${LIVEKIT_DIR}/credentials.env"
readonly LIVEKIT_CONFIG="${LIVEKIT_DIR}/livekit.yaml"
readonly NGINX_CONFIG="${LIVEKIT_DIR}/nginx.conf"
readonly LOCK_FILE="/var/lock/freechat-livekit.lock"
readonly TRANSACTION_DIR="${LIVEKIT_DIR}/deployment-transaction"
readonly MODE="${1:-deploy}"

log() {
  printf '[livekit-deploy] %s\n' "$*"
}

container_exists() {
  docker inspect "$1" >/dev/null 2>&1
}

container_running() {
  [ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null || true)" = "true" ]
}

wait_for_tcp() {
  local host="$1"
  local port="$2"
  local attempts="${3:-30}"

  for _ in $(seq 1 "$attempts"); do
    if timeout 1 bash -c "</dev/tcp/${host}/${port}" 2>/dev/null; then
      return 0
    fi
    sleep 1
  done
  return 1
}

rollback_pending_deployment() {
  [ -d "$TRANSACTION_DIR" ] || {
    log "No pending deployment to roll back"
    return 0
  }

  log "Rolling back the pending LiveKit deployment"

  if [ -f "${TRANSACTION_DIR}/livekit-changed" ]; then
    if [ -f "${TRANSACTION_DIR}/livekit-had-config" ]; then
      if [ -f "${LIVEKIT_CONFIG}.rollback" ]; then
        install -m 600 "${LIVEKIT_CONFIG}.rollback" "$LIVEKIT_CONFIG"
      fi
    else
      rm -f "$LIVEKIT_CONFIG"
    fi
    if [ -f "${TRANSACTION_DIR}/livekit-had-container" ]; then
      if container_exists freechat-livekit-rollback; then
        docker rm -f freechat-livekit >/dev/null 2>&1 || true
        docker rename freechat-livekit-rollback freechat-livekit
      elif ! container_exists freechat-livekit; then
        log "Cannot roll back LiveKit: retained container is missing"
        return 1
      fi
      docker start freechat-livekit >/dev/null
      wait_for_tcp 127.0.0.1 7880 30
    else
      docker rm -f freechat-livekit >/dev/null 2>&1 || true
    fi
  fi

  if [ -f "${TRANSACTION_DIR}/proxy-changed" ]; then
    if [ -f "${TRANSACTION_DIR}/proxy-had-config" ]; then
      if [ -f "${NGINX_CONFIG}.rollback" ]; then
        install -m 600 "${NGINX_CONFIG}.rollback" "$NGINX_CONFIG"
      fi
    else
      rm -f "$NGINX_CONFIG"
    fi
    if [ -f "${TRANSACTION_DIR}/proxy-had-container" ]; then
      if container_exists freechat-livekit-proxy-rollback; then
        docker rm -f freechat-livekit-proxy >/dev/null 2>&1 || true
        docker rename freechat-livekit-proxy-rollback freechat-livekit-proxy
      elif ! container_exists freechat-livekit-proxy; then
        log "Cannot roll back the TLS proxy: retained container is missing"
        return 1
      fi
      docker start freechat-livekit-proxy >/dev/null
      wait_for_tcp 127.0.0.1 443 30
    else
      docker rm -f freechat-livekit-proxy >/dev/null 2>&1 || true
    fi
  fi

  if [ -f "${TRANSACTION_DIR}/chat-changed" ]; then
    if cmp -s "$CHAT_CONFIG_ROLLBACK" "$CHAT_CONFIG" && \
      container_running freechat-chat && wait_for_tcp 127.0.0.1 10008 1; then
      log "Chat is already running with its restored configuration"
    else
      install -m 600 "$CHAT_CONFIG_ROLLBACK" "$CHAT_CONFIG"
      docker restart freechat-chat >/dev/null
      wait_for_tcp 127.0.0.1 10008 420
    fi
  fi

  if [ -f "${TRANSACTION_DIR}/renewal-changed" ]; then
    systemctl disable --now freechat-livekit-cert-renewal.timer >/dev/null 2>&1 || true

    if [ -f "${TRANSACTION_DIR}/renewal-script" ]; then
      install -m 700 "${TRANSACTION_DIR}/renewal-script" \
        /usr/local/sbin/renew-livekit-ip-cert
    else
      rm -f /usr/local/sbin/renew-livekit-ip-cert
    fi
    if [ -f "${TRANSACTION_DIR}/renewal-service" ]; then
      install -m 644 "${TRANSACTION_DIR}/renewal-service" \
        /etc/systemd/system/freechat-livekit-cert-renewal.service
    else
      rm -f /etc/systemd/system/freechat-livekit-cert-renewal.service
    fi
    if [ -f "${TRANSACTION_DIR}/renewal-timer" ]; then
      install -m 644 "${TRANSACTION_DIR}/renewal-timer" \
        /etc/systemd/system/freechat-livekit-cert-renewal.timer
    else
      rm -f /etc/systemd/system/freechat-livekit-cert-renewal.timer
    fi

    systemctl daemon-reload
    if [ -f "${TRANSACTION_DIR}/renewal-was-enabled" ]; then
      systemctl enable freechat-livekit-cert-renewal.timer >/dev/null
    fi
    if [ -f "${TRANSACTION_DIR}/renewal-was-active" ]; then
      systemctl start freechat-livekit-cert-renewal.timer
    fi
  fi

  if [ -f "${TRANSACTION_DIR}/firewall-changed" ]; then
    install -m 640 "${TRANSACTION_DIR}/ufw-user.rules" /etc/ufw/user.rules
    install -m 640 "${TRANSACTION_DIR}/ufw-user6.rules" /etc/ufw/user6.rules
    ufw reload >/dev/null
  fi

  rm -f "${LIVEKIT_CONFIG}.rollback" "${NGINX_CONFIG}.rollback" \
    "$CHAT_CONFIG_ROLLBACK" "${LIVEKIT_CONFIG}.next" "${NGINX_CONFIG}.next"
  rm -rf "$TRANSACTION_DIR"
  log "Rollback completed"
}

finalize_pending_deployment() {
  [ -d "$TRANSACTION_DIR" ] || {
    log "No pending deployment to finalize"
    return 0
  }

  container_running freechat-livekit
  container_running freechat-livekit-proxy
  container_running freechat-chat
  wait_for_tcp 127.0.0.1 7880 5
  wait_for_tcp 127.0.0.1 7881 5
  wait_for_tcp 127.0.0.1 443 5
  wait_for_tcp 127.0.0.1 10008 5

  docker rm -f freechat-livekit-rollback >/dev/null 2>&1 || true
  docker rm -f freechat-livekit-proxy-rollback >/dev/null 2>&1 || true
  rm -f "${LIVEKIT_CONFIG}.rollback" "${NGINX_CONFIG}.rollback" \
    "$CHAT_CONFIG_ROLLBACK"
  rm -rf "$TRANSACTION_DIR"
  log "Public validation passed; deployment finalized"
}

rollback_on_failed_exit() {
  local exit_code="$?"
  trap - EXIT
  if [ "$exit_code" -ne 0 ] && [ "$MODE" = "deploy" ] && \
    [ -d "$TRANSACTION_DIR" ]; then
    flock -u 9 >/dev/null 2>&1 || true
    if ! "$0" rollback; then
      log "Automatic rollback failed; transaction state was retained"
    fi
  fi
  exit "$exit_code"
}

trap rollback_on_failed_exit EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

exec 9>"$LOCK_FILE"
if ! flock -w 60 9; then
  log "Another LiveKit deployment or renewal is running"
  exit 1
fi

umask 077
install -d -m 700 "$LIVEKIT_DIR" "$CERTBOT_DIR" \
  "${CERTBOT_DIR}/lib" "${CERTBOT_DIR}/log"
install -d -m 755 "${CERTBOT_WEBROOT}/.well-known/acme-challenge"

for command in docker openssl python3 curl timeout flock systemctl; do
  command -v "$command" >/dev/null 2>&1 || {
    log "Required command is missing: ${command}"
    exit 1
  }
done

case "$MODE" in
  rollback)
    rollback_pending_deployment
    exit 0
    ;;
  finalize)
    finalize_pending_deployment
    exit 0
    ;;
  deploy)
    ;;
  *)
    log "Unknown mode: ${MODE} (expected deploy, finalize, or rollback)"
    exit 1
    ;;
esac

if [ -d "$TRANSACTION_DIR" ]; then
  log "A previous deployment was not finalized; rolling it back first"
  rollback_pending_deployment
fi
if container_exists freechat-livekit-rollback || \
  container_exists freechat-livekit-proxy-rollback; then
  log "Found rollback containers without transaction metadata; refusing to discard them"
  exit 1
fi
install -d -m 700 "$TRANSACTION_DIR"

if wait_for_tcp 127.0.0.1 443 1 && ! container_running freechat-livekit-proxy; then
  log "Port 443 is already occupied by a service not managed by this deployment"
  exit 1
fi
if wait_for_tcp 127.0.0.1 7880 1 && ! container_running freechat-livekit; then
  log "Port 7880 is already occupied by a service not managed by this deployment"
  exit 1
fi
if wait_for_tcp 127.0.0.1 7881 1 && ! container_running freechat-livekit; then
  log "Port 7881 is already occupied by a service not managed by this deployment"
  exit 1
fi

command -v ufw >/dev/null 2>&1 && ufw status | grep -q '^Status: active' || {
  log "UFW must be installed and active before changing production media services"
  exit 1
}
log "Configuring host firewall rules"
cp /etc/ufw/user.rules "${TRANSACTION_DIR}/ufw-user.rules"
cp /etc/ufw/user6.rules "${TRANSACTION_DIR}/ufw-user6.rules"
chmod 600 "${TRANSACTION_DIR}/ufw-user.rules" "${TRANSACTION_DIR}/ufw-user6.rules"
touch "${TRANSACTION_DIR}/firewall-changed"
ufw allow 80/tcp comment 'ACME HTTP challenge' >/dev/null
ufw allow 443/tcp comment 'LiveKit TLS' >/dev/null
ufw allow 7881/tcp comment 'LiveKit ICE TCP' >/dev/null
ufw allow 7882/udp comment 'LiveKit ICE UDP' >/dev/null
ufw delete allow 7880/tcp >/dev/null 2>&1 || true
ufw delete deny 7880/tcp >/dev/null 2>&1 || true
ufw insert 1 deny 7880/tcp comment 'LiveKit internal only' >/dev/null

challenge_name="livekit-preflight-$(openssl rand -hex 8)"
challenge_value="$(openssl rand -hex 16)"
challenge_path="${CERTBOT_WEBROOT}/.well-known/acme-challenge/${challenge_name}"
printf '%s' "$challenge_value" >"$challenge_path"
chmod 644 "$challenge_path"
served_value="$(curl --fail --silent --show-error --connect-timeout 5 \
  --resolve "${PUBLIC_IP}:80:127.0.0.1" \
  "http://${PUBLIC_IP}/.well-known/acme-challenge/${challenge_name}" || true)"
rm -f "$challenge_path"
if [ "$served_value" != "$challenge_value" ]; then
  log "The production Web container is not serving the persistent ACME challenge directory"
  exit 1
fi

if [ ! -s "$CREDENTIALS" ]; then
  log "Generating persistent LiveKit credentials"
  LIVEKIT_API_KEY="LK$(openssl rand -hex 12)"
  LIVEKIT_API_SECRET="$(openssl rand -hex 32)"
  printf 'LIVEKIT_API_KEY=%s\nLIVEKIT_API_SECRET=%s\n' \
    "$LIVEKIT_API_KEY" "$LIVEKIT_API_SECRET" >"$CREDENTIALS"
fi
chmod 600 "$CREDENTIALS"
# shellcheck disable=SC1090
. "$CREDENTIALS"
export LIVEKIT_API_KEY LIVEKIT_API_SECRET

log "Pulling pinned production images"
docker pull "$LIVEKIT_IMAGE" >/dev/null
docker pull "$NGINX_IMAGE" >/dev/null
docker pull "$CERTBOT_IMAGE" >/dev/null

issue_certificate() {
  local -a args=(
    certonly
    --non-interactive
    --agree-tos
    --register-unsafely-without-email
    --preferred-profile shortlived
    --webroot
    --webroot-path /var/www/certbot
    --ip-address "$PUBLIC_IP"
  )

  if [ -s "${CERT_PATH}/fullchain.pem" ]; then
    args+=(--force-renewal)
  fi

  docker run --rm \
    -v "${CERTBOT_DIR}:/etc/letsencrypt" \
    -v "${CERTBOT_DIR}/lib:/var/lib/letsencrypt" \
    -v "${CERTBOT_DIR}/log:/var/log/letsencrypt" \
    -v "${CERTBOT_WEBROOT}:/var/www/certbot" \
    "$CERTBOT_IMAGE" "${args[@]}"
}

if [ ! -s "${CERT_PATH}/fullchain.pem" ] || \
  ! openssl x509 -checkend 259200 -noout -in "${CERT_PATH}/fullchain.pem"; then
  log "Issuing or renewing the trusted IP certificate"
  issue_certificate
fi

openssl x509 -checkend 86400 -noout -in "${CERT_PATH}/fullchain.pem" || {
  log "The IP certificate is missing or expires in less than 24 hours"
  exit 1
}

cat >"${LIVEKIT_CONFIG}.next" <<EOF
port: 7880

rtc:
  node_ip: "${PUBLIC_IP}"
  use_external_ip: false
  tcp_port: 7881
  udp_port: 7882

keys:
  "${LIVEKIT_API_KEY}": "${LIVEKIT_API_SECRET}"
EOF
chmod 600 "${LIVEKIT_CONFIG}.next"

cat >"${NGINX_CONFIG}.next" <<EOF
events {}

http {
  server_tokens off;

  server {
    listen 443 ssl;
    server_name ${PUBLIC_IP};

    ssl_certificate /etc/letsencrypt/live/${PUBLIC_IP}/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/${PUBLIC_IP}/privkey.pem;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_session_cache shared:SSL:10m;
    ssl_session_timeout 1d;

    location / {
      proxy_pass http://127.0.0.1:7880;
      proxy_http_version 1.1;
      proxy_set_header Upgrade \$http_upgrade;
      proxy_set_header Connection "upgrade";
      proxy_set_header Host \$host;
      proxy_set_header X-Real-IP \$remote_addr;
      proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
      proxy_set_header X-Forwarded-Proto https;
      proxy_read_timeout 86400s;
      proxy_send_timeout 86400s;
    }
  }
}
EOF
chmod 600 "${NGINX_CONFIG}.next"

deploy_livekit() {
  local rollback_container="freechat-livekit-rollback"
  local rollback_config="${LIVEKIT_CONFIG}.rollback"

  touch "${TRANSACTION_DIR}/livekit-changed"

  if [ -f "$LIVEKIT_CONFIG" ]; then
    touch "${TRANSACTION_DIR}/livekit-had-config"
    cp "$LIVEKIT_CONFIG" "$rollback_config"
    chmod 600 "$rollback_config"
  fi

  if container_exists freechat-livekit; then
    touch "${TRANSACTION_DIR}/livekit-had-container"
    docker stop freechat-livekit >/dev/null || true
    docker rename freechat-livekit "$rollback_container"
  fi

  install -m 600 "${LIVEKIT_CONFIG}.next" "$LIVEKIT_CONFIG"

  if ! docker run -d --name freechat-livekit \
    --network host --restart=no \
    -v "${LIVEKIT_CONFIG}:/etc/livekit.yaml:ro" \
    "$LIVEKIT_IMAGE" --config /etc/livekit.yaml >/dev/null; then
    log "LiveKit container failed to start"
    return 1
  fi

  if ! wait_for_tcp 127.0.0.1 7880 30 || ! container_running freechat-livekit; then
    log "LiveKit did not become healthy"
    docker logs --tail 100 freechat-livekit || true
    return 1
  fi

  docker update --restart unless-stopped freechat-livekit >/dev/null
  rm -f "${LIVEKIT_CONFIG}.next"
}

if [ -f "$LIVEKIT_CONFIG" ] && \
  cmp -s "${LIVEKIT_CONFIG}.next" "$LIVEKIT_CONFIG" && \
  container_running freechat-livekit; then
  log "LiveKit configuration is unchanged; keeping the running media server"
  rm -f "${LIVEKIT_CONFIG}.next"
else
  log "Deploying LiveKit media server"
  deploy_livekit
fi

deploy_proxy() {
  local rollback_container="freechat-livekit-proxy-rollback"
  local rollback_config="${NGINX_CONFIG}.rollback"

  touch "${TRANSACTION_DIR}/proxy-changed"

  if [ -f "$NGINX_CONFIG" ]; then
    touch "${TRANSACTION_DIR}/proxy-had-config"
    cp "$NGINX_CONFIG" "$rollback_config"
    chmod 600 "$rollback_config"
  fi

  if container_exists freechat-livekit-proxy; then
    touch "${TRANSACTION_DIR}/proxy-had-container"
    docker stop freechat-livekit-proxy >/dev/null || true
    docker rename freechat-livekit-proxy "$rollback_container"
  fi

  install -m 600 "${NGINX_CONFIG}.next" "$NGINX_CONFIG"

  if ! docker run -d --name freechat-livekit-proxy \
    --network host --restart=no \
    -v "${NGINX_CONFIG}:/etc/nginx/nginx.conf:ro" \
    -v "${CERTBOT_DIR}:/etc/letsencrypt:ro" \
    "$NGINX_IMAGE" >/dev/null; then
    log "TLS proxy failed to start"
    return 1
  fi

  if ! wait_for_tcp 127.0.0.1 443 30 || \
    ! curl --silent --show-error --output /dev/null --connect-timeout 5 \
      --resolve "${PUBLIC_IP}:443:127.0.0.1" "https://${PUBLIC_IP}/"; then
    log "TLS proxy did not become healthy"
    docker logs --tail 100 freechat-livekit-proxy || true
    return 1
  fi

  docker update --restart unless-stopped freechat-livekit-proxy >/dev/null
  rm -f "${NGINX_CONFIG}.next"
}

if [ -f "$NGINX_CONFIG" ] && \
  cmp -s "${NGINX_CONFIG}.next" "$NGINX_CONFIG" && \
  container_running freechat-livekit-proxy; then
  log "TLS proxy configuration is unchanged; reloading its certificate"
  rm -f "${NGINX_CONFIG}.next"
  docker exec freechat-livekit-proxy nginx -t
  docker exec freechat-livekit-proxy nginx -s reload
else
  log "Deploying the LiveKit TLS proxy"
  deploy_proxy
fi

update_chat_config() {
  local rollback_config="$CHAT_CONFIG_ROLLBACK"
  local changed

  [ -f "$CHAT_CONFIG" ] || {
    log "Chat configuration is missing: ${CHAT_CONFIG}"
    return 1
  }
  container_exists freechat-chat || {
    log "Chat container is not deployed"
    return 1
  }

  cp "$CHAT_CONFIG" "$rollback_config"
  chmod 600 "$rollback_config"
  touch "${TRANSACTION_DIR}/chat-changed"
  export CHAT_CONFIG PUBLIC_IP

  changed="$(python3 <<'PY'
import os
import re
from pathlib import Path

path = Path(os.environ["CHAT_CONFIG"])
text = path.read_text(encoding="utf-8")
replacement = f'''liveKit:
  url: "wss://{os.environ["PUBLIC_IP"]}"
  backupUrls: [ "ws://127.0.0.1:7880" ]
  key: "{os.environ["LIVEKIT_API_KEY"]}"
  secret: "{os.environ["LIVEKIT_API_SECRET"]}"

'''
updated, count = re.subn(
    r"(?ms)^liveKit:\n.*?(?=^liveKitRecord:)",
    replacement,
    text,
    count=1,
)
if count != 1:
    raise SystemExit("liveKit block not found in chat-rpc-chat.yml")
if updated == text:
    print("false")
else:
    next_path = path.with_name(path.name + ".next")
    next_path.write_text(updated, encoding="utf-8")
    next_path.chmod(0o600)
    next_path.replace(path)
    print("true")
PY
)"
  chmod 600 "$CHAT_CONFIG"

  if [ "$changed" = "true" ]; then
    log "Restarting Chat with the production LiveKit endpoint"
    if ! docker restart freechat-chat >/dev/null || \
      ! wait_for_tcp 127.0.0.1 10008 420 || \
      ! container_running freechat-chat; then
      log "Chat failed its health check"
      return 1
    fi
  else
    log "Chat already uses the production LiveKit endpoint"
    rm -f "${TRANSACTION_DIR}/chat-changed" "$rollback_config"
  fi
}

update_chat_config

if [ -f /usr/local/sbin/renew-livekit-ip-cert ]; then
  cp /usr/local/sbin/renew-livekit-ip-cert "${TRANSACTION_DIR}/renewal-script"
fi
if [ -f /etc/systemd/system/freechat-livekit-cert-renewal.service ]; then
  cp /etc/systemd/system/freechat-livekit-cert-renewal.service \
    "${TRANSACTION_DIR}/renewal-service"
fi
if [ -f /etc/systemd/system/freechat-livekit-cert-renewal.timer ]; then
  cp /etc/systemd/system/freechat-livekit-cert-renewal.timer \
    "${TRANSACTION_DIR}/renewal-timer"
fi
if systemctl is-enabled freechat-livekit-cert-renewal.timer >/dev/null 2>&1; then
  touch "${TRANSACTION_DIR}/renewal-was-enabled"
fi
if systemctl is-active freechat-livekit-cert-renewal.timer >/dev/null 2>&1; then
  touch "${TRANSACTION_DIR}/renewal-was-active"
fi
touch "${TRANSACTION_DIR}/renewal-changed"

cat >"/usr/local/sbin/renew-livekit-ip-cert" <<'RENEW_SCRIPT'
#!/usr/bin/env bash
set -Eeuo pipefail

readonly PUBLIC_IP="8.148.66.77"
readonly CERTBOT_IMAGE="certbot/certbot:v5.4.0"
readonly CERTBOT_DIR="/data/app/certbot"
readonly CERTBOT_WEBROOT="${CERTBOT_DIR}/www"
readonly CERT_PATH="${CERTBOT_DIR}/live/${PUBLIC_IP}"

exec 9>/var/lock/freechat-livekit.lock
flock -n 9 || exit 0

if openssl x509 -checkend 259200 -noout -in "${CERT_PATH}/fullchain.pem"; then
  exit 0
fi

docker run --rm \
  -v "${CERTBOT_DIR}:/etc/letsencrypt" \
  -v "${CERTBOT_DIR}/lib:/var/lib/letsencrypt" \
  -v "${CERTBOT_DIR}/log:/var/log/letsencrypt" \
  -v "${CERTBOT_WEBROOT}:/var/www/certbot" \
  "$CERTBOT_IMAGE" certonly \
    --non-interactive \
    --agree-tos \
    --register-unsafely-without-email \
    --preferred-profile shortlived \
    --webroot \
    --webroot-path /var/www/certbot \
    --ip-address "$PUBLIC_IP" \
    --force-renewal

openssl x509 -checkend 86400 -noout -in "${CERT_PATH}/fullchain.pem"
docker exec freechat-livekit-proxy nginx -t
docker exec freechat-livekit-proxy nginx -s reload
RENEW_SCRIPT
chmod 700 /usr/local/sbin/renew-livekit-ip-cert

cat >/etc/systemd/system/freechat-livekit-cert-renewal.service <<'EOF'
[Unit]
Description=Renew the short-lived LiveKit IP certificate
After=docker.service network-online.target
Wants=network-online.target

[Service]
Type=oneshot
ExecStart=/usr/local/sbin/renew-livekit-ip-cert
EOF

cat >/etc/systemd/system/freechat-livekit-cert-renewal.timer <<'EOF'
[Unit]
Description=Check the LiveKit IP certificate twice daily

[Timer]
OnBootSec=10m
OnUnitActiveSec=12h
RandomizedDelaySec=30m
Persistent=true

[Install]
WantedBy=timers.target
EOF

systemctl daemon-reload
systemctl enable --now freechat-livekit-cert-renewal.timer >/dev/null

log "Running final local checks"
wait_for_tcp 127.0.0.1 7880 5
wait_for_tcp 127.0.0.1 7881 5
wait_for_tcp 127.0.0.1 443 5
curl --silent --show-error --output /dev/null --connect-timeout 5 \
  --resolve "${PUBLIC_IP}:443:127.0.0.1" "https://${PUBLIC_IP}/"
openssl x509 -noout -dates -in "${CERT_PATH}/fullchain.pem"
docker ps --filter name=freechat-livekit --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}'
systemctl list-timers freechat-livekit-cert-renewal.timer --no-pager
log "Local deployment passed; rollback state retained for public validation"
