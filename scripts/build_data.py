from sdg.open_sdg import open_sdg_build
from sdg import helpers
import yaml


def fix_time_period(x):
    x = str(x)
    years = x.split('-')
    return str(int(years[0]))


def set_time_detail(df):
    if 'TIME_DETAIL' not in df.columns.to_list():
        df = df.copy()
        df['TIME_DETAIL'] = df['Year']
    df['Year'] = df['Year'].apply(fix_time_period)
    return df


def columns_to_drop():
    return [
        'Gross Disbursement'
    ]


def common_alterations(df):
    df['REPORTING_TYPE'] = 'N'
    return df


def drop_columns(df):
    columns_in_data = df.columns.to_list()
    for column in columns_to_drop():
        if column in columns_in_data:
            df = df.drop([column], axis=1)
    return df


def set_series_and_unit(df, context):
    if 'SERIES' not in df.columns.to_list():
        indicator_id = context['indicator_id']
        series = helpers.sdmx.get_series_code_from_indicator_id(indicator_id)
        df['SERIES'] = series
    if 'UNIT_MEASURE' not in df.columns.to_list():
        df['UNIT_MEASURE'] = ''
    for index, row in df.iterrows():
        if row['UNIT_MEASURE'] == '':
            series = row['SERIES']
            unit = helpers.sdmx.get_unit_code_from_series_code(series)
            df.at[index, 'UNIT_MEASURE'] = unit
    return df


def alter_indicator_id(indicator_id):
    return indicator_id.replace('.', '-')


def alter_meta(meta):
    for key in meta:
        if meta[key] is not None and isinstance(meta[key], str):
            meta[key] = meta[key].replace("'", "&#39;")
    return meta


def apply_complex_mappings(df):
    try:
        filename = 'complex-jordan.yml'
        with open(filename, 'r') as stream:
            complex_mappings = yaml.load(stream, Loader=yaml.FullLoader)
            columns = df.columns.to_list()
            for mapping in complex_mappings:
                source_column = mapping['source_column']
                source_values = mapping['source_values']
                new_columns = source_values[next(iter(source_values))].keys()

                if source_column in columns:
                    def map_values(row):
                        source_value = row[source_column]
                        if source_value in source_values:
                            mapped_values = source_values[source_value]
                            for key in mapped_values:
                                row[key] = mapped_values[key]
                        return row
                    for column in new_columns:
                        df[column] = ''
                    df = df.apply(map_values, axis='columns')
                    df = df.drop(columns=[source_column])
        return df
    except:
        return df


def alter_data(df, context):
    column_fixes = {
        'Yeat': 'Year',
        'السنة': 'Year',
    }
    df = df.rename(columns=column_fixes)
    df = apply_complex_mappings(df)
    df = common_alterations(df)
    df = set_time_detail(df)
    df = drop_columns(df)
    df = set_series_and_unit(df, context)
    return df


config_path = 'config_data.yml'
open_sdg_build(config=config_path, alter_data=alter_data, alter_meta=alter_meta, alter_indicator_id=alter_indicator_id)
