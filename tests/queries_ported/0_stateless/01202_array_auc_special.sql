SELECT array_auc([], []); -- { serverError 43 }
SELECT array_auc([1], [1]);
SELECT array_auc([1], []); -- { serverError 43 }
SELECT array_auc([], [1]); -- { serverError 43 }
SELECT array_auc([1, 2], [3]); -- { serverError 36 }
SELECT array_auc([1], [2, 3]); -- { serverError 36 }
SELECT array_auc([1, 1], [1, 1]);
SELECT array_auc([1, 1], [0, 0]);
SELECT array_auc([1, 1], [0, 1]);
SELECT array_auc([0, 1], [0, 1]);
SELECT array_auc([1, 0], [0, 1]);
SELECT array_auc([0, 0, 1], [0, 1, 1]);
SELECT array_auc([0, 1, 1], [0, 1, 1]);
SELECT array_auc([0, 1, 1], [0, 0, 1]);
