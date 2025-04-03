# Multiple Attestation CA System
Code base for the 3 components of the multiple attestation CA system:
1. Fog Node - simulates certificate signing requests (CSRs) sent from multiple client fog nodes
2. CA server - 3 different Raspberry Pi 4B devices each host 1 CA server.
3. CA gateway - A single Desktop PC (HP Pro 400 G9 Core i7-13700 with 16GB RAM, 512GB SSD, and Windows 11). Hosts the CA gateway server that maintains a queue for fog node CSRs and forwards the queued requests to the 1 of the 3 CA servers using round-robin). All fog node CSRs are routed to the CA gateway and then to one of the CA servers.

