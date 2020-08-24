import pandas as pd
import config
import sys


def file(path):
    df = pd.read_csv(path, sep=";")

    protocol_statisctics = ((df.groupby('P2P PROTOCOL')['BYTES DOWNLINK'].sum()/(1024 ** 2) +
                             df.groupby('P2P PROTOCOL')['BYTES UPLINK'].sum()/(1024 ** 2))
                            .sort_values(ascending=False))

    print("Summury protocols statistics:", protocol_statisctics)

    actions_statisctics = ((df.groupby('SN CHARGING ACTION')['BYTES DOWNLINK'].sum()/(1024 ** 2) +
                            df.groupby('SN CHARGING ACTION')['BYTES UPLINK'].sum()/(1024 ** 2))
                           .sort_values(ascending=False))
    print("Summury charging actions statistics:", actions_statisctics)

    addresses = df.groupby('P2P PROTOCOL')['SERVER IP ADDRESS'].unique()
    instagram_addresses = addresses["instagram"]
    df_1 = df[df['SN CHARGING ACTION'] != 'Social-net']
    df_2 = df_1[df_1['SERVER IP ADDRESS'].isin(instagram_addresses)]
    summury_traffic = ((df_2.groupby('SN CHARGING ACTION')['BYTES DOWNLINK']).sum() +
                       (df_2.groupby('SN CHARGING ACTION')['BYTES UPLINK']).sum())/(1024**2)

    return f"Summury traffic for return: {summury_traffic.sum()} MB"


def database():
    pass


def main():
    try:
        path = sys.argv[1]
        print(file(path))
    except IndexError:
        print(database())


if __name__ == '__main__':
    main()
