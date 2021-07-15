# DataWig Design Document
Julian Rakuschek, Noah Ruhmer
### Basic Idea
Let us assume the following table as presented in the corresponding Paper by Prof. Biessmann [1]:

| Type          |      Description     |   Size         | Color         |
| :-----------: | :------------------- | :------------: | :-----------: |
|  Shoes        | ideal for running    | 12UK           | Black         |
| SD Card       | for saving files     | 8GB            | Blue          |
| Dress         | This yellow dress ...| M              | ???           |

The goal is obviously to impute the Color `Yellow` for the Dress, but how do we get there?

First we select the feature columns which are `Type`, `Description` and `Size`, the label column alias to-be-imputed shall be `Color`

#### Numerical Encoding x<sup>c</sup>
The first stage is the transformation from strings and categorical data into their numerical representation:

| Type          |      Description     |   Size         | Color         |
| :-----------: | :------------------- | :------------: | :-----------: |
|  OHE        | Sequential Encoding    | OHE           | OHE         |

Here we can use a One-Hot Encoder (OHE) for categorical data and a sequential encoding for strings. As SystemDS already has the bultin function `toOneHot` we plan on using that and for the sequential data we would implement a new builtin function `sequentialEncode` which would look like this:
* Let's say Row 1 contains "Shoes" and Row2 "SD Card"
* First we assign each unique character a unique index, e.g.
    * `{S: 1, h: 2, o: 3, e: 4, s: 5, D: 6, " ": 7, C: 8, a: 9, r: 10, d: 11}`
* Then we replace each character in the string with the corresponding token-index, which would yield two arrays:
     * `[1, 2, 3, 4, 5]`
     * `[1, 6, 7, 8, 9, 10, 11]`
* This is also the way how the SequentialEncoder in the Python-Implementation of DataWig works

#### Feature Extraction
This is the part where we yet don't fully understand on how this should work. In the paper [1], Prof. Biessmann uses one Featurizer for each column, namely for One-hot encoded data he uses an embedding and for sequential data they either use: "an n-gram representation or a character-based embedding using a long short-term memory (LSTM) recurrent neural network" [1, chapter 3]

However, we think that it could also be possible to use the PCA algorithm which is already implemented in SystemDS as a Feature Extraction Layer. What we yet don't know is whether we should apply PCA to all columns at once and reduce them to a dimension of one, or use PCA on each column like Prof. Biessmann.

| Type          |      Description     |   Size         |
| :-----------: | :------------------- | :------------: |
|  PCA        | PCA   | PCA           |

The resulting columns would then all be concatenated to a single vector X, but in case we use PCA on all columns at once, this step can be omitted.

Also a side note: In the table above you can see that we only apply the Feature Extraction to the feature columns, the label-column will then be used in the Imputation Equation.

#### Imputation
For the imputation we have to calculate the probability of each possible missing value replacement and choose the likeliest value: `P(y | X, Θ)`
Thereby we want the probability of y fitting in the column given the Feature Extraction *and* the trained parameter Θ

Θ is then calculated with the equation presentation in Prof. Biessmanns paper [1], if we understand that right, we calculate this equation once every epoch. The remaining question is what the parameters in Θ stand for, namely they are: `Θ = (W, z, b)`


[1] https://dl.acm.org/doi/10.1145/3269206.3272005