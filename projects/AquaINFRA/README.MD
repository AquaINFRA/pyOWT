sudo docker run -v ./projects/AquaINFRA/out:/app/projects/AquaINFRA/out owt-classificat
ion-image --input "https://raw.githubusercontent.com/bishun945/pyOWT/refs/heads/main/projects/AquaINFRA/data/Rrs_demo_AquaINFRA_hy
per.csv" --input_option "csv" --sensor "HYPER" --output_option "1" --output "out/test.txt"

