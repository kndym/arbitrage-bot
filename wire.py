import openvpn_api
v = openvpn_api.VPN('localhost', 7505)

v.connect()
# Do some stuff, e.g.
print(v.release)
v.disconnect()