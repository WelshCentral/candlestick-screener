import pandas as pd
import datetime
import random
from deap import algorithms, base, creator, tools
import sys 

from data_fetcher import DataFetcher

collateral = 11500
stocks = None

def evaluate(individual):
    score = 0
    used_coll, income, beta = 0, 0, 0
    count = 0
    for i in range(len(individual)):
        if individual[i] == 1:
            stock = stocks.iloc[i]
            used_coll += stock["option_strike"]*100
            beta += (1 - abs(stock["beta"])) # distance from 1: low = good
            income += stock["option_income"]   # maximize income
            count += 1  # average beta is important, not total beta
    if count > 0:
        beta = beta/count   # count = number of stocks
    # score is designed to be minimized 
    score = 0.1*income + beta 
    if used_coll > collateral:
        score -= used_coll - collateral
    return score,

def random_sample(pop):
    return random.sample(pop, 1)[0]


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python3 optimize.py <collateral>")
        exit()
    collateral = float(sys.argv[1])

    fetcher = DataFetcher('weekly_option_tickers.csv', datetime.datetime(2021, 3, 12), 11500, strikes_out=2)
    data = fetcher.fetch_data() 
    data = data.dropna()
    stocks = data

    # 95% chance of 0, 5% chance of 1
    population_values = [0]*95 + [1]*5
    random.shuffle(population_values)

    creator.create("FitnessMax", base.Fitness, weights=(1.0,))
    creator.create("Individual", list, fitness=creator.FitnessMax)

    toolbox = base.Toolbox()
    toolbox.register("attr_bool", random_sample, population_values)
    toolbox.register("individual", tools.initRepeat, creator.Individual, toolbox.attr_bool, n=stocks.shape[0])
    toolbox.register("population", tools.initRepeat, list, toolbox.individual)
    toolbox.register("evaluate", evaluate)
    toolbox.register("mate", tools.cxTwoPoint)
    toolbox.register("mutate", tools.mutFlipBit, indpb=0.05)
    toolbox.register("select", tools.selTournament, tournsize=3)

    pop = toolbox.population(n=300)
    algorithms.eaSimple(pop, toolbox, cxpb=0.5, mutpb=0.2, ngen=50000, verbose=True)
    best = tools.selBest(pop, k=1)[0]

    income, collateral, beta = 0, 0, 0
    count = 0
    for i in range(len(best)):
        if best[i] == 1:
            print("bought: ticker={} price={} beta={} income={}".format(
                    stocks.iloc[i]["symbol"], stocks.iloc[i]["price_mid"], stocks.iloc[i]["beta"], stocks.iloc[i]["option_income"]))
            income += stocks.iloc[i]["option_income"]
            beta += float(stocks.iloc[i]["beta"])
            collateral += stocks.iloc[i]["price_mid"]*100
            count += 1
    if count > 0:
        beta /= count
    print("totals: income={}, beta={}, collateral={}, percent income={}".format(income, beta, collateral, ((100*income)/collateral)*100))
    