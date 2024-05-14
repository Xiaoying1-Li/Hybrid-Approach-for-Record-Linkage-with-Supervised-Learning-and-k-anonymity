import pandas as pd
import recordlinkage as rl
from recordlinkage.index import Block
from recordlinkage.base import BaseCompareFeature
import numpy as np
import matplotlib.pyplot as plt

def load_data(file_a, file_b):
    df_a = pd.read_csv(file_a)
    df_b = pd.read_csv(file_b)
    return df_a, df_b

def extract_postcode_range(df):
    df[['postcode_min', 'postcode_max']] = df['postcode'].str.extract(r'\[(\d+)-(\d+)\]').astype(int)
    return df

def create_multi_index(df_a, df_b, pairs):
    matching_pairs = []
    for i, j in pairs:
        a_min, a_max = df_a.loc[i, ['postcode_min', 'postcode_max']]
        b_min, b_max = df_b.loc[j, ['postcode_min', 'postcode_max']]
        if a_min < b_max and b_min < a_max:
            matching_pairs.append((df_a.index[i], df_b.index[j]))

    multi_index = pd.MultiIndex.from_tuples(matching_pairs, names=['index_a', 'index_b'])
    return multi_index

class CompareEuclideanDistance(BaseCompareFeature):
    def __init__(self, left_on, right_on, *args, **kwargs):
        super().__init__(left_on, right_on, *args, **kwargs)

    def _compute_vectorized(self, s1, s2):
        print("s1:", s1)
        print("s2:", s2)
        distance = np.sqrt((s1 - s2) ** 2)

        min_dist = np.min(distance)
        max_dist = np.max(distance)
        normalized_distance = 1 - (distance - min_dist) / (max_dist - min_dist)

        return normalized_distance

def preprocess_data(df):
    df['street_number'] = df['street_number'].apply(process_range_string).astype(int)
    df['postcode'] = df['postcode'].apply(process_range_string).astype(int)
    df['date_of_birth'] = df['date_of_birth'].apply(process_range_string).astype(int)
    return df

def process_range_string(range_string):
    range_values = [int(i) for i in range_string.strip('[]').replace(' ', '').split('-')]
    return sum(range_values) / len(range_values)

def compare_records(multi_index, df_a, df_b):
    comp = rl.Compare()
    comp.add(CompareEuclideanDistance('postcode', 'postcode'))
    comp.add(CompareEuclideanDistance('street_number', 'street_number'))
    comp.add(CompareEuclideanDistance('date_of_birth', 'date_of_birth'))
    comp.string('state','state',method ='jaro')
    comp.exact('salary-class','salary-class')
    comp.compute(multi_index, df_a, df_b)
    return comp.compute(multi_index, df_a, df_b)

def categorize_matches(comparison_result, threshold_match):
    total_similarity = comparison_result.sum(axis=1)
    matches_index = total_similarity[total_similarity >= threshold_match].index
    possible_matches_index = total_similarity[total_similarity.between(threshold_match - 1, threshold_match - 0.1)].index
    non_matches_index = total_similarity[total_similarity < threshold_match - 1].index
    return matches_index, possible_matches_index, non_matches_index


def visualize_threshold_effect(total_similarity, threshold_range):
    match_counts = []
    possible_match_counts = []
    not_match_counts = []

    for threshold in threshold_range:
        matches_index = total_similarity[total_similarity >= threshold].index
        possible_matches_index = total_similarity[total_similarity.between(threshold - 1, threshold - 0.1)].index
        non_matches_index = total_similarity[total_similarity < threshold - 1].index

        match_counts.append(len(matches_index))
        possible_match_counts.append(len(possible_matches_index))
        not_match_counts.append(len(non_matches_index))

    plt.figure(figsize=(10, 6))
    plt.plot(threshold_range, match_counts, label='Match')
    plt.plot(threshold_range, possible_match_counts, label='Possible Match')
    plt.plot(threshold_range, not_match_counts, label='Not Match')
    plt.xlabel('Threshold')
    plt.ylabel('Count')
    plt.title('Number of Matches, Possible Matches, and Not Matches vs. Threshold')
    plt.legend()
    plt.grid(True)
    plt.show()


def main():
    # Load data
    df_a, df_b = load_data('/Users/lixiaoying/Desktop/Masterarbeit/Master/k-anonymization/output/output_dataset_a.csv',
                           '/Users/lixiaoying/Desktop/Masterarbeit/Master/k-anonymization/output/output_dataset_b.csv')

    # Extract postcode range
    df_a = extract_postcode_range(df_a)
    df_b = extract_postcode_range(df_b)

    # Define indexer and perform blocking
    indexer = rl.Index()
    indexer.add(Block('postcode_min'))
    indexer.add(Block('postcode_max'))
    pairs = indexer.index(df_a, df_b)

    # Create MultiIndex and save as CSV
    multi_index = create_multi_index(df_a, df_b, pairs)
    multi_index_df = multi_index.to_frame(index=False)
    multi_index_df.columns = ['index_a', 'index_b']
    #multi_index_df['index_a'] = df_a.iloc[multi_index_df['index_a']]['index'].values
    #multi_index_df['index_b'] = df_b.iloc[multi_index_df['index_b']]['index'].values
    #multi_index_df.to_csv(r'index_output/multi_index.csv', index=False)
    print(multi_index)

    # Preprocess data
    df_a = preprocess_data(df_a)
    df_b = preprocess_data(df_b)

    # Calculate total comparison pairs
    total_comparison_pairs = len(df_a) * len(df_b)

    # Calculate reduced comparison pairs
    reduced_comparison_pairs = len(multi_index)
    print("Total Comparison Pairs:", total_comparison_pairs)
    print("Reduced Comparison Pairs:", reduced_comparison_pairs)

    # Calculate blocking efficiency
    blocking_efficiency = (total_comparison_pairs - reduced_comparison_pairs) / total_comparison_pairs
    print("Blocking Efficiency: {:.2%}".format(blocking_efficiency))

    # Compare records and save as CSV
    variable_names = ['postcode', 'street_number', 'date_of_birth', 'state', 'salary-class']
    comparison_result = compare_records(multi_index, df_a, df_b)
    comparison_result.columns = variable_names
    #comparison_result_df = comparison_result.reset_index()
    #comparison_result_df['index_a'] = df_a.loc[comparison_result_df['index_a'], 'index'].values
    #comparison_result_df['index_b'] = df_b.loc[comparison_result_df['index_b'], 'index'].values
    #comparison_result_df.set_index(['index_a', 'index_b'], inplace=True)
    #comparison_result_df.to_csv(r'index_output/comparison_result.csv')
    #print(comparison_result)

    # Categorize matches
    threshold_match = 4.5
    matches_index, possible_matches_index, non_matches_index = categorize_matches(comparison_result, threshold_match)

    # Output match categories
    print("match:")
    #print(matches_index)
    match = list(zip(df_a.iloc[matches_index.get_level_values('index_a').values, 0].values, df_b.iloc[matches_index.get_level_values('index_b').values, 0].values))
    #match_df = pd.DataFrame(match, columns = ['index_a', 'index_b'])
   # match_df.to_csv(r'index_output/match.csv', index=False)
    print(match)
    print("\npossible match:")
    #print(possible_matches_index)
    possible_match = list(zip(df_a.iloc[possible_matches_index.get_level_values('index_a').values, 0].values,
                   df_b.iloc[possible_matches_index.get_level_values('index_b').values, 0].values))
    #possible_match_df = pd.DataFrame(possible_match, columns = ['index_a','index_b'])
    #possible_match_df.to_csv(r'index_output/possible_match.csv', index = False)
    print(possible_match)
    print("\nnot match:")
    #print(non_matches_index)
    not_match = list(zip(df_a.iloc[non_matches_index.get_level_values('index_a').values, 0].values,
                   df_b.iloc[non_matches_index.get_level_values('index_b').values, 0].values))
    #not_match_df = pd.DataFrame(not_match, columns = ['index_a','index_b'])
    #not_match_df.to_csv(r'index_output/not_match.csv', index = False)
    print(not_match)

    # Visualize threshold effect
    threshold_range = np.linspace(1, 5, num=100)
    visualize_threshold_effect(comparison_result.sum(axis=1), threshold_range)

if __name__ == "__main__":
    main()
