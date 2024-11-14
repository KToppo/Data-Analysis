from nselib import capital_market
import pandas as pd
from tqdm import tqdm
import numpy as np



def data_modeling(symbols:pd.Series):
    filtered_data = pd.DataFrame()
    total_itration = len(symbols)
    with tqdm(total=total_itration) as progress_bar:
        for symbol in symbols.values:
            data = capital_market.price_volume_and_deliverable_position_data(symbol=symbol.upper(),from_date="14-11-2021", to_date="14-11-2024")
            # data = capital_market.price_volume_and_deliverable_position_data(symbol=symbol.upper(), period="1Y")
            data = data.loc[data["Series"]=="EQ"]
            filtered_data[symbol] = data["ClosePrice"].replace(",","").astype("float").values
            progress_bar.update()
    filtered_data = filtered_data.dropna()
    return filtered_data

def expected_risk(weight, cov_matrix):
    return np.sqrt(weight.T @ cov_matrix @weight)

def expected_return(weight, return_list):
    return np.sum(return_list.mean()*weight)*len(return_list.index)# len(return_list.index) => No. of dayes traded in year


def risk_and_return(portfoliyo:dict): # portfoliyo = {"share_1":amount invested (int), "share_2":amount invested(int), .......}
    share_list = pd.Series(portfoliyo.keys()).str.upper()
    sum_weights = np.sum(list(portfoliyo.values()))
    weights = np.array([(w/sum_weights) for w in list(portfoliyo.values())])#(amount invested / sum of weights), (amount invested 2/ sum of weights), ..........

    portfoliyo_data = data_modeling(share_list)

    returns = round(np.log(portfoliyo_data/portfoliyo_data.shift(1)),4) # 0.1387, -1.9172, .........
    returns = returns.dropna()

    covarence_matrix = returns.cov()*len(portfoliyo_data.index)

    estimated_risk = expected_risk(weight=weights, cov_matrix=covarence_matrix)
    estimated_return = expected_return(weight=weights, return_list=returns)

    return round(estimated_risk*100,2), round(estimated_return*100,2)

# EXAMPLE
symbols = {"ntpc":300, "nhpc":200, "itc":100}
risk, returns = risk_and_return(symbols)

print(f"\n\nEstimated risk of upcoming year: {risk}%\n",
      f"Estimated return of upcoming year: {returns}%")
