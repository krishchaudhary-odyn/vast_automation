from vastai_sdk import VastAI
from tabulate import tabulate


import json
import time
import socket
import subprocess

def ssh_port_open(host, port, timeout=3):
	try:
		with socket.create_connection((host, port), timeout=timeout):
			return True
	except OSError:
		return False


def main():
	print("Pulling in latest data from Vast.ai...")
	time.sleep(2)

	vast_sdk = VastAI(api_key="35395aefc138be98efd808042e6f06f36f860a2b96436ea9515241ffa64552a5")
	
	key_name = "platform-key"

	public_key = open("vast_platform_key.pub").read().strip()

	vast_sdk.create_ssh_key(
			name=key_name,
			ssh_key=public_key
	)

	print(vast_sdk.show_ssh_keys())

	while True:
		time.sleep(2)
		available_gpus = vast_sdk.search_offers()

		print(json.dumps(available_gpus, indent=2))

		rows = []
		for o in available_gpus:
				rows.append([
						o.get("id"),
						o.get("gpu_name"),
						o.get("num_gpus"),
						o.get("dph_total"),
						o.get("geolocation"),
						o.get("reliability2")
				])

		headers = ["ID", "GPU", "#GPUs", "$/hr", "Location", "Reliability"]

		print(tabulate(rows, headers=headers, tablefmt="pretty"))

		inp = input("Enter the id of the instance you prefer or -1 for creating your own specified instance: ")

		if inp != "-1":
			offer_id = int(inp)
			selected_offer = None
			for offer in available_gpus:
				if offer['id'] == offer_id:
					selected_offer = offer
					break

			if selected_offer:
				print(f"You have selected the offer with ID: {offer_id}. Building instance...")

				# Proceed with further actions like booking the offer
				vast_platform_key = open("vast_platform_key.pub").read().strip()
				instance = vast_sdk.create_instance(
					id=offer_id,
					image="nvidia/cuda:12.1.1-runtime-ubuntu22.04",
					disk=50,
					image_runtype="ssh",
					ssh_key=vast_platform_key
				)

				instance_id = instance["new_contract"]
				print("Instance ID:", instance_id)

				print("Waiting for instance to be SSH-ready...")

				# Loop to wait for the instance to be running
				info = vast_sdk.show_instance(id=instance_id)
				while info["actual_status"] != "running":
					info = vast_sdk.show_instance(id=instance_id)

					ssh_host = info.get("ssh_host")
					ssh_port = info.get("ssh_port")
					state = info.get("state")

					time.sleep(5)

				print(f"ssh -i vast_platform_key root@{ssh_host} -p {ssh_port}")
				ssh_command = ["ssh", "-i", "vast_platform_key", f"root@{ssh_host}", "-p", str(ssh_port)]
				subprocess.run(ssh_command)
				return
			else:
				print(f"No offer found with ID: {offer_id}")
				time.sleep(3)
				continue

if __name__ == "__main__":
	main()