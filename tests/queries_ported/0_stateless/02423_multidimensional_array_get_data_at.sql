SELECT format_row('RawBLOB', [[[33]], []]); -- { serverError 48 }
SELECT format_row('RawBLOB', [[[]], []]); -- { serverError 48 }
SELECT format_row('RawBLOB', [[[[[[[0x48, 0x65, 0x6c, 0x6c, 0x6f]]]]]], []]); -- { serverError 48 }
SELECT format_row('RawBLOB', []::array(array(nothing))); -- { serverError 48 }
SELECT format_row('RawBLOB', [[], [['Hello']]]); -- { serverError 48 }
SELECT format_row('RawBLOB', [[['World']], []]); -- { serverError 48 }
SELECT format_row('RawBLOB', []::array(string)); -- { serverError 48 }
