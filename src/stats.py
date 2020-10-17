import pandas as pd
import matplotlib.pyplot as plt


def ns_occurrence():
    df = pd.read_csv('../data/out/csv/data.csv', names=["Id", "Title", "NS", "Model", "Format"], skiprows=1)
    ns = df["NS"].value_counts()

    print(ns)

    ns.plot.barh()
    plt.show()


def model_occurrence():
    df = pd.read_csv('../data/out/csv/data.csv', names=["Id", "Title", "NS", "Model", "Format"], skiprows=1)
    model = df["Model"].value_counts()
    print(model)


def format_occurrence():
    df = pd.read_csv('../data/out/csv/data.csv', names=["Id", "Title", "NS", "Model", "Format"], skiprows=1)
    format = df["Format"].value_counts()
    print(format)


def count():
    df = pd.read_csv('../data/out/csv/data.csv', names=["Id", "Title", "NS", "Model", "Format"], skiprows=1)
    ns = df["Id"].count()
    print(ns)


if __name__ == "__main__":

    model_occurrence()

    format_occurrence()

    count()

    ns_occurrence()
