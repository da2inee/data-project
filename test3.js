/**
 * 배열에서 짝수만 필터링하는 함수
 * 
 * @param {number[]} arr - 필터링할 숫자 배열
 * @returns {number[]} 짝수만 포함된 새로운 배열
 * 
 * @example
 * filterEvenNumbers([1, 2, 3, 4, 5, 6]);
 * // 결과: [2, 4, 6]
 */
function filterEvenNumbers(arr) {
    return arr.filter(num => num % 2 === 0);
}
