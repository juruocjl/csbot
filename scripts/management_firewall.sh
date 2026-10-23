#!/usr/bin/env bash
# Keep management listeners off the public NIC, including Docker forwarding.
set -euo pipefail
interface="${1:-eth0}"
[[ "$interface" =~ ^[a-zA-Z0-9_.:-]+$ ]] || exit 2
tcp_ports="22,1053,1234,5432,6099,8081,8888,9090,10011"
for firewall in iptables ip6tables; do
    "$firewall" -w -N CSBOT-MANAGEMENT 2>/dev/null || "$firewall" -w -S CSBOT-MANAGEMENT >/dev/null
    "$firewall" -w -F CSBOT-MANAGEMENT
    "$firewall" -w -A CSBOT-MANAGEMENT -m conntrack --ctstate ESTABLISHED,RELATED -j RETURN
    "$firewall" -w -A CSBOT-MANAGEMENT -p tcp -m multiport --dports "$tcp_ports" -j DROP
    "$firewall" -w -A CSBOT-MANAGEMENT -p udp -m multiport --dports 1053,123 -j DROP
    "$firewall" -w -A CSBOT-MANAGEMENT -j RETURN
    "$firewall" -w -C INPUT -i "$interface" -j CSBOT-MANAGEMENT 2>/dev/null || \
        "$firewall" -w -I INPUT 1 -i "$interface" -j CSBOT-MANAGEMENT
    # FORWARD is checked after DNAT: match the original destination port.
    "$firewall" -w -N CSBOT-DOCKER-MGMT 2>/dev/null || "$firewall" -w -S CSBOT-DOCKER-MGMT >/dev/null
    "$firewall" -w -F CSBOT-DOCKER-MGMT
    "$firewall" -w -A CSBOT-DOCKER-MGMT -m conntrack --ctstate ESTABLISHED,RELATED -j RETURN
    for port in 1234 5432 6099 8081; do
        "$firewall" -w -A CSBOT-DOCKER-MGMT -p tcp -m conntrack --ctorigdstport "$port" -j DROP
    done
    "$firewall" -w -A CSBOT-DOCKER-MGMT -j RETURN
    "$firewall" -w -C FORWARD -i "$interface" -j CSBOT-DOCKER-MGMT 2>/dev/null || \
        "$firewall" -w -I FORWARD 1 -i "$interface" -j CSBOT-DOCKER-MGMT
done
