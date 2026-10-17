# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Download real integer-valued series and save each as int64 .npy plus a manifest row.

Usage: python -I fetch.py <data_dir> [source ...]
"""

import io
import json
import os
import subprocess
import sys
import zipfile
from decimal import Decimal

import numpy as np

DATA = sys.argv[1]
RAW = os.path.join(DATA, "raw")
OUT = os.path.join(DATA, "series")
os.makedirs(RAW, exist_ok=True)
os.makedirs(OUT, exist_ok=True)
MANIFEST = os.path.join(DATA, "manifest.jsonl")


def curl(url, path, timeout=600):
    path = os.path.join(RAW, path)
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        subprocess.run(["curl", "-gsSL", "--fail", "-m", str(timeout), "-o", path, url], check=True)
    return path


def save(name, category, values, rate, note, scale=1):
    v = np.asarray(values, dtype=np.int64)
    np.save(os.path.join(OUT, name + ".npy"), v)
    row = dict(name=name, category=category, n=int(v.size), rate=rate, scale=scale, note=note)
    with open(MANIFEST, "a") as f:
        f.write(json.dumps(row) + "\n")
    print(f"{name:40s} n={v.size:9d} min={v.min()} max={v.max()}")


def decimals_to_int(strings):
    """Scale decimal strings to exact integers using the largest number of decimals seen."""
    strings = [s.strip() for s in strings]
    d = max((len(s.split(".")[1]) if "." in s else 0) for s in strings)
    f = 10**d
    return np.array([int(Decimal(s) * f) for s in strings], dtype=np.int64), f


# --- Seismology: IRIS FDSN, raw digitizer counts (int32) ---------------------------------------
def iris():
    import obspy

    reqs = [
        ("seis_IU_ANMO_BHZ_40hz", "IU", "ANMO", "00", "BHZ", "2024-01-01T00:00:00", "2024-01-01T06:00:00"),
        ("seis_IU_ANMO_LHZ_1hz", "IU", "ANMO", "00", "LHZ", "2024-01-01T00:00:00", "2024-01-11T00:00:00"),
        ("seis_IU_COLA_BH1_40hz", "IU", "COLA", "00", "BH1", "2024-01-01T00:00:00", "2024-01-01T06:00:00"),
        ("seis_IU_ANMO_HHZ_100hz", "IU", "ANMO", "00", "HHZ", "2024-01-01T00:00:00", "2024-01-01T02:00:00"),
        # Noto peninsula M7.5, 2024-01-01 07:10 UTC, recorded at a Japanese GSN station.
        ("seis_IU_MAJO_BHZ_quake", "IU", "MAJO", "00", "BHZ", "2024-01-01T07:05:00", "2024-01-01T09:05:00"),
        ("seis_IU_ANMO_HNZ_strongmotion", "IU", "ANMO", "20", "HNZ", "2024-01-01T00:00:00", "2024-01-01T02:00:00"),
        ("seis_IU_ANMO_LDO_pressure", "IU", "ANMO", "30", "LDO", "2024-01-01T00:00:00", "2024-01-08T00:00:00"),
        ("seis_IU_ANMO_BDF_infrasound", "IU", "ANMO", "32", "BDF", "2024-01-01T00:00:00", "2024-01-01T06:00:00"),
        ("seis_IU_ANMO_LFZ_magnetic", "IU", "ANMO", "40", "LFZ", "2024-01-01T00:00:00", "2024-01-08T00:00:00"),
        ("seis_IU_ANMO_LWS_windspeed", "IU", "ANMO", "50", "LWS", "2024-01-01T00:00:00", "2024-01-15T00:00:00"),
        ("seis_IU_ANMO_LKO_temp", "IU", "ANMO", "50", "LKO", "2024-01-01T00:00:00", "2024-01-15T00:00:00"),
    ]
    for name, net, sta, loc, cha, t0, t1 in reqs:
        url = (
            "https://service.iris.edu/fdsnws/dataselect/1/query?"
            f"net={net}&sta={sta}&loc={loc}&cha={cha}&starttime={t0}&endtime={t1}"
        )
        try:
            p = curl(url, name + ".mseed")
            st = obspy.read(p)
            st.merge(method=1, fill_value="interpolate")
            tr = max(st, key=lambda t: t.stats.npts)
            save(name, "seismic", tr.data, tr.stats.sampling_rate, f"IRIS {net}.{sta}.{loc}.{cha} {t0}..{t1} counts")
        except Exception as e:  # noqa: BLE001
            print("FAILED", name, e)


# --- NOAA CO-OPS water level (tides), metres with 3 decimals -> mm ---------------------------------
def tides():
    for sta, label in [("9414290", "sanfrancisco"), ("8518750", "thebattery"), ("9447130", "seattle")]:
        vals = []
        for m in range(1, 13):
            begin = f"2023{m:02d}01"
            end = f"2023{m:02d}{[31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][m - 1]}"
            url = (
                "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?"
                f"begin_date={begin}&end_date={end}&station={sta}&product=water_level&datum=MLLW"
                "&units=metric&time_zone=gmt&format=csv&application=research"
            )
            p = curl(url, f"tide_{sta}_{m}.csv")
            for line in open(p).read().splitlines()[1:]:
                f = line.split(",")
                if len(f) > 1 and f[1].strip():
                    vals.append(f[1])
        v, s = decimals_to_int(vals)
        save(f"tide_{label}_6min", "tides", v, 1 / 360, f"NOAA CO-OPS {sta} 2023 water level, 6 min", s)
    # one-minute water level, max 4 days per request
    vals = []
    for d in range(1, 29, 4):
        url = (
            "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?"
            f"begin_date=202302{d:02d}&end_date=202302{d + 3:02d}&station=9414290&product=one_minute_water_level"
            "&datum=MLLW&units=metric&time_zone=gmt&format=csv&application=research"
        )
        p = curl(url, f"tide1m_{d}.csv")
        for line in open(p).read().splitlines()[1:]:
            f = line.split(",")
            if len(f) > 1 and f[1].strip():
                vals.append(f[1])
    v, s = decimals_to_int(vals)
    save("tide_sanfrancisco_1min", "tides", v, 1 / 60, "NOAA CO-OPS 9414290 Feb 2023, 1 min", s)


# --- USGS geomagnetism, 1 Hz, nT with 2 decimals -------------------------------------------------
def geomag():
    for obs in ["BOU", "FRD"]:
        for el in ["H", "Z"]:
            url = (
                f"https://geomag.usgs.gov/ws/data/?id={obs}&starttime=2024-03-01T00:00:00Z"
                f"&endtime=2024-03-01T23:59:59Z&elements={el}&sampling_period=1&format=iaga2002"
            )
            try:
                p = curl(url, f"geomag_{obs}_{el}.txt")
            except Exception as e:  # noqa: BLE001
                print("FAILED", obs, el, e)
                continue
            vals = []
            for line in open(p):
                if line[:2] == "20":
                    x = line.split()[3]
                    if not x.startswith("99999"):
                        vals.append(x)
            v, s = decimals_to_int(vals)
            save(f"geomag_{obs}_{el}_1hz", "geomagnetic", v, 1, f"USGS {obs} {el} 2024-03-01 1 s", s)


# --- PhysioNet biomedical waveforms (raw ADC units) ----------------------------------------------
def physionet():
    import wfdb

    def rec(db, record, chans, name, cat, sampto=None):
        d = os.path.join(RAW, "physionet", db)
        os.makedirs(d, exist_ok=True)
        try:
            r = wfdb.rdrecord(record, pn_dir=db, physical=False, sampto=sampto, channel_names=chans)
        except Exception as e:  # noqa: BLE001
            print("FAILED", name, e)
            return
        for i, ch in enumerate(r.sig_name):
            save(
                f"{name}_{ch}".replace(" ", "").replace(",", ""),
                cat,
                r.d_signal[:, i],
                r.fs,
                f"PhysioNet {db}/{record} {ch} ADC",
            )

    rec("mitdb", "100", ["MLII", "V5"], "ecg_mitdb100", "ecg")
    rec("mitdb", "203", ["MLII"], "ecg_mitdb203", "ecg")
    rec("ptbdb/patient001", "s0010_re", ["i", "v2"], "ecg_ptb1khz", "ecg")
    rec("bidmc", "bidmc01", ["PLETH,", "RESP,", "II,"], "bidmc01", "ppg_resp", None)
    import pyedflib

    for db, path, chans, name in [
        ("eegmmidb", "S001/S001R01.edf", ["Fc5.", "Cz.."], "eeg_mmidb_160hz"),
        ("chbmit", "chb01/chb01_03.edf", ["FP1-F7", "T7-P7"], "eeg_chbmit_256hz"),
    ]:
        try:
            p = curl(f"https://physionet.org/files/{db}/1.0.0/{path}", name + ".edf")
            f = pyedflib.EdfReader(p)
            labels = f.getSignalLabels()
            for ch in chans:
                i = labels.index(ch)
                save(
                    f"{name}_{ch.strip('.')}",
                    "eeg",
                    f.readSignal(i, digital=True),
                    f.getSampleFrequency(i),
                    f"PhysioNet {db}/{path} {ch} digital",
                )
            f.close()
        except Exception as e:  # noqa: BLE001
            print("FAILED", name, e)
    rec("fantasia", "f1o01", ["RESP"], "resp_fantasia", "ppg_resp", 250 * 3600)
    rec("apnea-ecg", "a01", None, "ecg_apnea100hz", "ecg", 100 * 3600)


# --- Audio (int16 PCM) ----------------------------------------------------------------------------
def audio():
    import soundfile as sf

    base = "https://huggingface.co/datasets/hf-internal-testing/librispeech_asr_dummy/resolve/main/clean/validation-00000-of-00001.parquet"
    try:
        import pyarrow.parquet as pq

        p = curl(base, "librispeech_dummy.parquet")
        t = pq.read_table(p).to_pylist()
        pcm = []
        for row in t[:20]:
            a = row["audio"]
            data, sr = sf.read(io.BytesIO(a["bytes"]), dtype="int16")
            pcm.append(data)
        save("audio_librispeech_speech", "audio", np.concatenate(pcm), sr, "LibriSpeech dummy, 20 utterances int16")
    except Exception as e:  # noqa: BLE001
        print("FAILED librispeech", e)


def music():
    import pyarrow.parquet as pq
    import soundfile as sf

    for ds, label, picks in [
        (
            "sanchit-gandhi/gtzan",
            "gtzan",
            {0: "blues", 150: "classical", 30: "blues2", 120: "classical2", 60: "row60", 90: "row90"},
        ),
        ("ashraq/esc50", "esc50", {0: "a", 1: "b", 2: "c", 3: "d", 4: "e", 5: "f", 6: "g", 7: "h"}),
    ]:
        url = f"https://huggingface.co/api/datasets/{ds}/parquet/default/train/0.parquet"
        try:
            p = curl(url, f"{label}_0.parquet")
            t = pq.read_table(p)
            col = [c for c in t.column_names if c == "audio"][0]
            rows = t.column(col).to_pylist()
            if label == "esc50":
                pcm = []
                for i in picks:
                    data, sr = sf.read(io.BytesIO(rows[i]["bytes"]), dtype="int16")
                    pcm.append(data if data.ndim == 1 else data[:, 0])
                save("audio_esc50_env8clips", "audio", np.concatenate(pcm), sr, "ESC-50 first 8 clips int16")
                continue
            genre = t.column("genre").to_pylist() if "genre" in t.column_names else None
            for i, g in picks.items():
                if i >= len(rows):
                    continue
                data, sr = sf.read(io.BytesIO(rows[i]["bytes"]), dtype="int16")
                data = data if data.ndim == 1 else data[:, 0]
                save(f"audio_gtzan_{g}_{i}", "audio", data, sr, f"GTZAN row {i} genre={genre[i] if genre else g} int16")
        except Exception as e:  # noqa: BLE001
            print("FAILED", ds, e)


# --- Jena climate (10 min weather station) ---------------------------------------------------------
def jena():
    p = curl("https://storage.googleapis.com/tensorflow/tf-keras-datasets/jena_climate_2009_2016.csv.zip", "jena.zip")
    z = zipfile.ZipFile(p)
    lines = z.read("jena_climate_2009_2016.csv").decode().splitlines()
    hdr = [h.strip('"') for h in lines[0].split(",")]
    rows = [line.split(",") for line in lines[1:]]
    for col, label in [("p (mbar)", "pressure"), ("T (degC)", "temp"), ("rh (%)", "humidity"), ("wv (m/s)", "wind")]:
        i = hdr.index(col)
        v, s = decimals_to_int([r[i] for r in rows])
        save(f"weather_jena_{label}_10min", "weather", v, 1 / 600, f"Jena climate 2009-2016 {col}", s)


# --- UCI household power (1 min) -------------------------------------------------------------------
def household():
    p = curl(
        "https://archive.ics.uci.edu/static/public/235/individual+household+electric+power+consumption.zip",
        "household.zip",
    )
    z = zipfile.ZipFile(p)
    lines = z.read("household_power_consumption.txt").decode().splitlines()
    hdr = lines[0].split(";")
    rows = [line.split(";") for line in lines[1:] if "?" not in line]
    for col in ["Global_active_power", "Voltage", "Global_intensity"]:
        i = hdr.index(col)
        v, s = decimals_to_int([r[i] for r in rows])
        save(f"power_household_{col.lower()}_1min", "power", v, 1 / 60, f"UCI household power {col}", s)


# --- Binance trades (financial tick data; random-walk control) -------------------------------------
def binance():
    p = curl(
        "https://data.binance.vision/data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2024-01-01.zip", "btc_trades.zip"
    )
    z = zipfile.ZipFile(p)
    lines = z.read(z.namelist()[0]).decode().splitlines()[:2_000_000]
    rows = [line.split(",") for line in lines]
    v, s = decimals_to_int([r[1] for r in rows])
    save("fin_btcusdt_trade_price", "financial", v, 0, "Binance BTCUSDT trades 2024-01-01 price", s)
    save("fin_btcusdt_trade_time", "timestamp", [int(r[4]) for r in rows], 0, "Binance BTCUSDT trades time (ms)")


# --- CWRU bearing vibration (float, recovered to ADC counts) ---------------------------------------
def cwru():
    import scipy.io

    for fid, label in [("97", "normal"), ("105", "innerrace007")]:
        p = curl(f"https://engineering.case.edu/sites/default/files/{fid}.mat", f"cwru_{fid}.mat")
        m = scipy.io.loadmat(p)
        k = [k for k in m if k.endswith("DE_time")][0]
        x = m[k].ravel()
        q = np.min(np.abs(np.diff(np.unique(x))))
        ints = np.round(x / q)
        err = np.max(np.abs(ints * q - x))
        save(f"vib_cwru_{label}_12khz", "vibration", ints, 12000, f"CWRU {fid} DE accel, x/{q:.3e} (max err {err:.1e})")


SOURCES = dict(
    iris=iris,
    tides=tides,
    geomag=geomag,
    physionet=physionet,
    audio=audio,
    music=music,
    jena=jena,
    household=household,
    binance=binance,
    cwru=cwru,
)


def more():
    import obspy
    import pyedflib
    import scipy.io
    import soundfile as sf
    import wfdb

    for name, net, sta, loc, cha, t0, t1 in [
        ("seis_II_PFO_BHZ_40hz", "II", "PFO", "00", "BHZ", "2024-02-01T00:00:00", "2024-02-01T06:00:00"),
        ("seis_IU_KONO_BHE_40hz", "IU", "KONO", "00", "BHE", "2024-02-01T00:00:00", "2024-02-01T06:00:00"),
        ("seis_GE_DAV_BHZ_20hz", "GE", "DAV", "--", "BHZ", "2024-02-01T00:00:00", "2024-02-01T12:00:00"),
        ("seis_IU_SNZO_HH1_100hz", "IU", "SNZO", "00", "HH1", "2024-02-01T00:00:00", "2024-02-01T02:00:00"),
        ("seis_IU_ANMO_VHZ_0.1hz", "IU", "ANMO", "00", "VHZ", "2023-06-01T00:00:00", "2024-01-01T00:00:00"),
        ("seis_IU_ANMO_LH1_1hz", "IU", "ANMO", "10", "LH1", "2024-02-01T00:00:00", "2024-02-11T00:00:00"),
    ]:
        url = (
            "https://service.iris.edu/fdsnws/dataselect/1/query?"
            f"net={net}&sta={sta}&loc={loc}&cha={cha}&starttime={t0}&endtime={t1}"
        )
        try:
            st = obspy.read(curl(url, name + ".mseed"))
            st.merge(method=1, fill_value="interpolate")
            tr = max(st, key=lambda t: t.stats.npts)
            save(name, "seismic", tr.data, tr.stats.sampling_rate, f"IRIS {net}.{sta}.{loc}.{cha} {t0}..{t1} counts")
        except Exception as e:  # noqa: BLE001
            print("FAILED", name, e)

    pcm = []
    for i in range(1, 9):
        try:
            d, sr = sf.read(
                curl(f"https://physionet.org/files/challenge-2016/1.0.0/training-a/a{i:04d}.wav", f"pcg_a{i:04d}.wav"),
                dtype="int16",
            )
            pcm.append(d)
        except Exception as e:  # noqa: BLE001
            print("FAILED pcg", i, e)
    if pcm:
        save(
            "pcg_heartsound_2khz", "pcg", np.concatenate(pcm), sr, "PhysioNet challenge-2016 training-a a0001-a0008 wav"
        )

    for rec in ["emg_healthy", "emg_neuropathy"]:
        try:
            r = wfdb.rdrecord(rec, pn_dir="emgdb", physical=False)
            save(rec + "_4khz", "emg", r.d_signal[:, 0], r.fs, f"PhysioNet emgdb/{rec} ADC")
        except Exception as e:  # noqa: BLE001
            print("FAILED", rec, e)

    try:
        p = curl("https://physionet.org/files/sleep-edfx/1.0.0/sleep-cassette/SC4001E0-PSG.edf", "sleep_SC4001E0.edf")
        f = pyedflib.EdfReader(p)
        labels = f.getSignalLabels()
        for ch, label in [
            ("EEG Fpz-Cz", "eeg_sleep_fpzcz_100hz"),
            ("EOG horizontal", "eog_sleep_100hz"),
            ("Resp oro-nasal", "resp_sleep_1hz"),
        ]:
            i = labels.index(ch)
            cat = "eeg" if label.startswith(("eeg", "eog")) else "ppg_resp"
            save(
                label, cat, f.readSignal(i, digital=True), f.getSampleFrequency(i), f"sleep-edfx SC4001E0 {ch} digital"
            )
    except Exception as e:  # noqa: BLE001
        print("FAILED sleep", e)

    for rec in ["LabWalks/co001_base", "LabWalks/fl001_base"]:
        try:
            d, r = rec.split("/")
            w = wfdb.rdrecord(r, pn_dir=f"ltmm/{d}", physical=False)
            for i, ch in enumerate(w.sig_name[:3]):
                save(f"imu_ltmm_{r}_{ch}", "imu", w.d_signal[:, i], w.fs, f"PhysioNet ltmm/{rec} {ch} ADC")
        except Exception as e:  # noqa: BLE001
            print("FAILED ltmm", rec, e)

    for fid, label in [("118", "ball007"), ("130", "outerrace007"), ("234", "outerrace021")]:
        try:
            m = scipy.io.loadmat(curl(f"https://engineering.case.edu/sites/default/files/{fid}.mat", f"cwru_{fid}.mat"))
            k = [k for k in m if k.endswith("DE_time")][0]
            x = m[k].ravel()
            q = np.min(np.abs(np.diff(np.unique(x))))
            ints = np.round(x / q)
            err = np.max(np.abs(ints * q - x))
            save(
                f"vib_cwru_{label}_12khz",
                "vibration",
                ints,
                12000,
                f"CWRU {fid} DE accel, x/{q:.3e} (max err {err:.1e})",
            )
        except Exception as e:  # noqa: BLE001
            print("FAILED cwru", fid, e)

    import pyarrow.parquet as pq

    try:
        t = pq.read_table(
            curl(
                "https://huggingface.co/api/datasets/sanchit-gandhi/gtzan/parquet/default/train/1.parquet",
                "gtzan_1.parquet",
            )
        )
        rows = t.column("audio").to_pylist()
        for i in range(0, len(rows), max(1, len(rows) // 6))[:6]:
            d, sr = sf.read(io.BytesIO(rows[i]["bytes"]), dtype="int16")
            save(f"audio_gtzan_p1_{i}", "audio", d if d.ndim == 1 else d[:, 0], sr, f"GTZAN parquet 1 row {i} int16")
    except Exception as e:  # noqa: BLE001
        print("FAILED gtzan1", e)


SOURCES["more"] = more


def grid():
    """30 kHz appliance current/voltage (PLAID) and 500 kHz grid voltage (ADMIT)."""
    import h5py

    p = curl("https://ndownloader.figshare.com/files/18183113", "plaid_submetered.zip", timeout=3600)
    z = zipfile.ZipFile(p)
    for rec in ["1005", "1464", "1312", "788", "759", "1816"]:
        rows = [line.split(",") for line in z.read(f"submetered_new/{rec}.csv").decode().split()]
        for c, label in [(0, "current"), (1, "voltage")]:
            if label == "voltage" and rec not in ("1005", "1312"):
                continue
            v, s = decimals_to_int([r[c] for r in rows])
            save(f"grid_plaid_{rec}_{label}_30khz", "grid", v, 30000, f"PLAID 2018 submetered/{rec}.csv {label}", s)

    p = curl("https://zenodo.org/records/14723239/files/data_set_ADMIT.mat?download=1", "admit.mat")
    a = h5py.File(p)["voltage"][:]
    q = np.min(np.diff(np.unique(a[:, 0])))
    ints = np.round(a.T.ravel() / q)
    err = np.max(np.abs(ints * q - a.T.ravel()))
    save("grid_admit_voltage_500khz", "grid", ints, 500000, f"ADMIT 5 x 0.04 s voltage, x/{q:.4e} (max err {err:.1e})")


SOURCES["grid"] = grid


def daphnet():
    """Daphnet freezing-of-gait: 3 body-worn accelerometers at 64 Hz, integer mg."""
    z = zipfile.ZipFile(
        curl("https://archive.ics.uci.edu/static/public/245/daphnet+freezing+of+gait.zip", "daphnet.zip")
    )
    for rec, col, label in [
        ("S01R01", 1, "ankle_fwd"),
        ("S01R01", 5, "thigh_vert"),
        ("S01R01", 9, "trunk_lat"),
        ("S02R01", 3, "ankle_lat"),
        ("S03R01", 8, "trunk_vert"),
    ]:
        rows = np.loadtxt(io.BytesIO(z.read(f"dataset_fog_release/dataset/{rec}.txt")), dtype=np.int64)
        save(f"imu_daphnet_{rec}_{label}_64hz", "imu", rows[:, col], 64, f"Daphnet {rec} column {col} (mg)")
        if label == "ankle_fwd":
            save(f"ts_daphnet_{rec}_ms", "timestamp", rows[:, 0], 64, f"Daphnet {rec} time (ms)")


SOURCES["daphnet"] = daphnet


def waves():
    """CDIP Datawell wave-buoy displacement at 1.28 Hz: float32 metres on an exact 1 cm grid."""
    for stn, label in [("100p1", "torreypines"), ("106p1", "waimea"), ("029p1", "pointreyes"), ("166p1", "papa")]:
        for axis in ["Z", "X"]:
            name = f"wave_cdip{stn[:3]}_{label}_{axis.lower()}_1.28hz"
            url = (
                f"https://thredds.cdip.ucsd.edu/thredds/dodsC/cdip/archive/{stn}/{stn}_d01.nc.ascii"
                f"?xyz{axis}Displacement[2000000:1:{2000000 + (1 << 20) - 1}]"
            )
            try:
                text = open(curl(url, name + ".ascii", timeout=1800)).read()
                body = text.split("xyz" + axis + "Displacement", 2)[-1]
                vals = np.array(
                    [float(v) for v in body.replace("\n", ",").split(",")[1:] if v.strip()], dtype=np.float64
                )
                vals = vals[vals > -900]
                cm = np.round(vals * 100)
                err = np.max(np.abs(cm / 100 - vals))
                save(name, "waves", cm, 1.28, f"CDIP {stn} d01 xyz{axis}Displacement (cm, max err {err:.1e} m)")
            except Exception as e:  # noqa: BLE001
                print("FAILED", name, e)


SOURCES["waves"] = waves


def nsynth():
    """NSynth test set: 4 s single notes, 16 kHz 16-bit PCM. Only acoustically recorded instruments."""
    import tarfile
    from collections import defaultdict

    import soundfile as sf

    p = curl(
        "http://download.magenta.tensorflow.org/datasets/nsynth/nsynth-test.jsonwav.tar.gz", "nsynth-test.tar.gz", 3600
    )
    notes = defaultdict(list)
    with tarfile.open(p) as tf:
        for m in tf:
            base = os.path.basename(m.name)
            if not base.endswith(".wav"):
                continue
            family, source = base.split("_")[:2]
            if source != "acoustic":
                continue
            data, sr = sf.read(io.BytesIO(tf.extractfile(m).read()), dtype="int16")
            notes[family].append((base, data))
    for family, items in sorted(notes.items()):
        items.sort()
        pcm = np.concatenate([d for _, d in items[:24]])
        save(
            f"audio_nsynth_{family}_acoustic",
            "audio_notes",
            pcm,
            16000,
            f"NSynth test, {min(24, len(items))} acoustic {family} notes, 16 kHz int16",
        )


SOURCES["nsynth"] = nsynth


def nsynth_high():
    """High-register acoustic NSynth notes, sampled evenly (every k-th note by name) so nothing is
    cherry-picked: pitch >= 72 (fundamental >= 523 Hz, <= 31 samples/cycle) and pitch >= 96."""
    import tarfile

    import soundfile as sf

    p = curl(
        "http://download.magenta.tensorflow.org/datasets/nsynth/nsynth-test.jsonwav.tar.gz", "nsynth-test.tar.gz", 3600
    )
    notes = []
    with tarfile.open(p) as tf:
        for m in tf:
            base = os.path.basename(m.name)
            if base.endswith(".wav") and "_acoustic_" in base:
                pitch = int(base.rsplit("-", 2)[1])
                if pitch >= 72:
                    notes.append((base, pitch, sf.read(io.BytesIO(tf.extractfile(m).read()), dtype="int16")[0]))
    notes.sort()
    for lo, step, label in [(72, 10, "pitch72plus"), (96, 3, "pitch96plus")]:
        picked = [n for n in notes if n[1] >= lo][::step]
        for i in range(0, len(picked) - 15, 16):
            group = picked[i : i + 16]
            save(
                f"audio_nsynth_{label}_{i // 16}",
                "audio_notes",
                np.concatenate([g[2] for g in group]),
                16000,
                f"NSynth test acoustic notes, pitch >= {lo}, every {step}th by name, notes {i}..{i + 15}: "
                + " ".join(g[0][:-4] for g in group),
            )


SOURCES["nsynth_high"] = nsynth_high


if __name__ == "__main__":
    for s in sys.argv[2:] or SOURCES:
        print("==", s)
        try:
            SOURCES[s]()
        except Exception as e:  # noqa: BLE001
            print("FAILED source", s, e)
