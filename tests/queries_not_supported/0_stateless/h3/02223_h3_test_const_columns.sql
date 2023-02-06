-- Tags: no-fasttest

select round(geoToH3(to_float64(0),to_float64(1),array_join([1,2])), 2);
select h3ToParent(641573946153969375, array_join([1,2]));
SELECT round(h3HexAreaM2(array_join([1,2])), 2);
SELECT round(h3HexAreaKm2(array_join([1,2])), 2);
SELECT round(h3CellAreaM2(array_join([579205133326352383,589753847883235327,594082350283882495])), 2);
SELECT NULL, to_float64('-1'), -2147483648, h3CellAreaM2(array_join([9223372036854775807, 65535, NULL]));  -- { serverError 117 }
SELECT round(h3CellAreaRads2(array_join([579205133326352383,589753847883235327,594082350283882495])), 2);
SELECT NULL, to_float64('-1'), -2147483648, h3CellAreaRads2(array_join([9223372036854775807, 65535, NULL]));  -- { serverError 117 }
SELECT h3GetResolution(array_join([579205133326352383,589753847883235327,594082350283882495]));
SELECT round(h3EdgeAngle(array_join([0,1,2])), 2);
SELECT round(h3EdgeLengthM(array_join([0,1,2])), 2);
SELECT round(h3EdgeLengthKm(array_join([0,1,2])), 2);
WITH h3ToGeo(array_join([579205133326352383,589753847883235327,594082350283882495])) AS p SELECT round(p.1, 2), round(p.2, 2);
SELECT array_map(p -> (round(p.1, 2), round(p.2, 2)), h3ToGeoBoundary(array_join([579205133326352383,589753847883235327,594082350283882495])));
SELECT h3kRing(array_join([579205133326352383]), array_join([to_uint16(1),to_uint16(2),to_uint16(3)]));
SELECT h3GetBaseCell(array_join([579205133326352383,589753847883235327,594082350283882495]));
SELECT h3IndexesAreNeighbors(617420388351344639, array_join([617420388352655359, 617420388351344639, 617420388352917503]));
SELECT h3ToChildren(599405990164561919, array_join([6,5]));
SELECT h3ToParent(599405990164561919, array_join([0,1]));
SELECT h3ToString(array_join([579205133326352383,589753847883235327,594082350283882495]));
SELECT stringToH3(h3ToString(array_join([579205133326352383,589753847883235327,594082350283882495])));
SELECT h3IsResClassIII(array_join([579205133326352383,589753847883235327,594082350283882495]));
SELECT h3IsPentagon(array_join([stringToH3('8f28308280f18f2'),stringToH3('821c07fffffffff'),stringToH3('0x8f28308280f18f2L'),stringToH3('0x821c07fffffffffL')]));
SELECT h3GetFaces(array_join([stringToH3('8f28308280f18f2'),stringToH3('821c07fffffffff'),stringToH3('0x8f28308280f18f2L'),stringToH3('0x821c07fffffffffL')]));
SELECT h3ToCenterChild(577023702256844799, array_join([1,2,3]));
SELECT round(h3ExactEdgeLengthM(array_join([1298057039473278975,1370114633511206911,1442172227549134847,1514229821587062783])), 2);
SELECT round(h3ExactEdgeLengthKm(array_join([1298057039473278975,1370114633511206911,1442172227549134847,1514229821587062783])), 2);
SELECT round(h3ExactEdgeLengthRads(array_join([1298057039473278975,1370114633511206911,1442172227549134847,1514229821587062783])), 2);
SELECT h3NumHexagons(array_join([1,2,3]));
SELECT h3Line(array_join([stringToH3('85283473fffffff')]), array_join([stringToH3('8528342bfffffff')]));
SELECT h3HexRing(array_join([579205133326352383]), array_join([to_uint16(1),to_uint16(2),to_uint16(3)])); -- { serverError 117 }
SELECT h3HexRing(array_join([581276613233082367]), array_join([to_uint16(0),to_uint16(1),to_uint16(2)]));
