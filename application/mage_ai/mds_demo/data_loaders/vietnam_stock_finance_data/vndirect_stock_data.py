from vnstock import *

@data_loader
def load_fs_data(**kwargs) -> DataFrame:
    """
    Template for loading Financial Statement data

    """
    symbol = 'SSI'
    report_type = 'IncomeStatement'
    frequency = 'Yearly'

    df = financial_report (symbol=symbol, report_type=report_type, frequency=frequency)

    return df
