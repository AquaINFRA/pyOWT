# Docker commands

`docker build -t owt-classification-image .`

`docker run -it -v ./in:/in -v ./out:/out owt-classification-image --input "https://raw.githubusercontent.com/AquaINFRA/pyOWT/refs/heads/main/projects/AquaINFRA/data/Rrs_demo_AquaINFRA_hyper.csv" --input_option "csv" --sensor "HYPER" --output "/out/tmp1.csv" --output_option "1"`