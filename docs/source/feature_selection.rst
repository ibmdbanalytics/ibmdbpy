Feature Selection
*****************

Ibmdbpy includes a range of functions to conduct advanced data analysis, such as estimating the relevance of attributes, to make a prediction. This is important to improve the quality of classifiers and in-database algorithms. Hereafter we provide the documentation for each developed functions. 

The implemented functions are: Pearson correlation, Sperman rank correlation, T-statistics, Chi-squared statistics, Gini index as well as several entropy-based measures such as information gain, gain ratio and symmetric uncertainty. We also provide a wrapper to discretize continuous attributes. 

.. currentmodule:: ibmdbpy.feature_selection

Pearson correlation
-------------------

The Pearson correlation coefficient was introduced by Karl Pearson in 1886. It is a measure of the linear correlation between two random variables X and Y. Its values range from -1 to 1. The value 0 indicates no linear correlation between X and Y. The value -1 indicates a total negative correlation and +1 indicates a total positive correlation between X and Y. It is defined on real-value variables.

The main drawback of the Pearson correlation coefficient is that it can only detect linear correlations. The Pearson correlation coefficient is a parametric measure, since it assumes that the distribution of each attribute can be described using a gaussian distribution. In the real world, correlations are not necessarily of linear nature.

.. currentmodule:: ibmdbpy.feature_selection
.. autofunction:: pearson

Spearman rank correlation
-------------------------

Spearman rank correlation, also called grade correlation, is a non-parametric measure of statistical dependence. It assesses how well the relationship between two random variables X and Y can be described using a monotonous function. It corresponds to the Pearson correlation applied to the rank of two real random variables X and Y.

The Spearman rank correlation is interesting because it is not limited to measuring linear relationships and can be applied to discrete ordinal values. Note however that it still cannot be applied to categorical attributes, such as non-numerical value.

.. autofunction:: spearman

T-statistics
------------

The T-test is a statistical test often used to determine whether the means of two samples
are significantly different from each other, taking into account the difference between
the means and the variability of the samples. The t-test has been extensively used for
feature ranking, especially in the field of microarray analysis.

T-statistics, as it is implemented here, requires the target attribute to be categorical (nominal or numerical discrete) and the other attributes to be numerical, such that the mean can be computed.

.. autofunction:: ttest

Chi-Squared statistics
----------------------

The Chi-Squared method evaluates each feature individually by measuring its Chi-squared statistic with respect to the classes of another feature. We can calculate the Chi-Squared values for each pair of attributes, a larger Chi-squared value typically means a larger inter-dependence between the two features. Since it applies to categorical attributes, numerical attributes require first to be discretized into several intervals.

.. autofunction:: chisquared

Gini index
----------

The Gini index, also known as the Gini coefficient or Gini ratio, is a measure commonly used in decision trees to decide what is the best attribute to split the current node for an efficient decision tree construction. It was developed by Corrado Gini in 1912. The Gini index is a measure of statistical dispersion and can be interpreted as a measure of impurity for an attribute.

The Gini index values range from 0 to 1. The greater the value, the more the classes of an attributes are evenly distributed. An attribute having a smaller Gini index value typically means that it is easier to predict apriori, because one or several classes are more frequent than others.

.. autofunction:: gini

More interestingly, we can measure how well knowing the value of a particular attribute X can improve the average Gini index value of each of the samples of an attribute Y, partitioned with respect to the classes of X. This is dened as the conditional Gini index measure.

.. autofunction:: gini_pairwise


Entropy
-------

Entropy is an important concept of information theory. It was introduced by Claude Schannon in 1948 and corresponds to the expected quantity of the information contained in a flow of information. Entropy can be understood as a measure of uncertainty of a random variable. 

Intuitively, an attribute with a higher entropy will be more difficult to predict apriori than other attributes with a lower entropy. Various correlation measures are based on the information-theoretical concept of entropy, such as information gain, gain ratio and symmetric uncertainty. We will discuss those measures in the next sections.

.. autofunction:: entropy

.. autofunction:: entropy_stats

Information gain
----------------

In information theory, information gain is often used as a synonym for mutual information. It is a measure of mutual dependence between two variables X and Y and gives an interpretation of the amount of information that is shared by the two variables.

.. autofunction:: info_gain

Gain ratio
----------

The information gain ratio is a variant of the mutual information. It can be seen as a normalization of the mutual information values from 0 to 1. It is the ratio of information to the entropy of the target attribute. By doing so, it also reduces the bias toward attributes with many values. There exists several versions of the definition of this measure. Especially, there exists an asymmetric and a symmetric version.

.. autofunction:: gain_ratio

Symmetric uncertainty
---------------------

Symmetric uncertainty is a pair-wise independence measure originally defined by Witten and Franck. Symmetric uncertainty compensates the bias of information gain and offers a variant to the symmetric gain ratio normalized within the range [0, 1] with the value 1 indicating that knowledge of either one of the values completely predicts the value of the other and the value 0 indicating that X and Y are independent. It is also a symmetric measure.

.. autofunction:: su

Discretization
--------------

Since most correlation measures require the attributes to be discretized first, we provide a wrapper for an in-database discretization method. 

.. autofunction:: discretize