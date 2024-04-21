## Introduction

We want to explore whether there exists a hybrid approach combining k-anonymization and supervised learning to improve the correctness of private record linkage.

Our idea is as follows:

1. At first clean the data and then anonymize it.

2. Using Blocking Step to reduce data comparison.

3. Using similarity computation and threshold evaluation to categorize the data into 'Match', 'Mismatch', and Possible Match'.

4. Using supervised learning algorithm to train the data in 'Match' and 'Mismatch' and then re-categorize the data in 'Possible Match'.

5. Adjusting the parameters, optimize the algorithm and evaluate the results.

6. Using Stremlit to create a web application for users to upload data and then perform record linkage.

