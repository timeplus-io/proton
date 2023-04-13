SELECT if(1, 0, throw_if(1, 'Executing FALSE branch'));
SELECT if(empty(''), 0, throw_if(1, 'Executing FALSE branch'));
