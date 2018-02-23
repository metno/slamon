### slamon - script to monitor published model run results from thredds.met.no

```sh
git clone https://github.com/metno/slamon.git && \
cd slamon && \
virtualenv --python=python3 . && \
. bin/activate && \
pip install -r requirements.txt && \
./slamon.py --dry-run -v config.json
```
