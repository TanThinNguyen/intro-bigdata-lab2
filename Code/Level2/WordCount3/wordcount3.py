from mrjob.job import MRJob
from mrjob.step import MRStep
import re

# pattern regex để tìm các từ chỉ gồm chữ cái, không gồm chữ số và kí tự khác
WORD_REGEX = re.compile(r"[a-zA-Z]+")

# class MRJWordCount cài đặt job đếm từ, 
# kế thừa và override lại hàm của lớp MRJob có sẵn
class MRMostUsedWord(MRJob):

    def steps(self):
        # Hàm trả về list các step: ở đây job gồm 2 step
        # - step 1: gồm cả mapper, combiner, reducer -> đếm số lượng xuất hiện của các từ
        # - step 2: chỉ có reducer -> tìm từ xuất hiện nhiều nhất
        # MRStep nhận tham số là các hàm cài đặt tương ứng
        # cho mapper, combiner, reducer
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_max_word)
        ]

    # tách ra các từ xuất hiện ở mỗi câu
    def mapper_get_words(self, _, line):
        # loại bỏ khoảng trắng đầu và cuối dòng
        line = line.strip()
        # tạo đầu ra là các cặp <key, value> dạng <word, 1> cho mỗi từ trong 1 line
        for word in WORD_REGEX.findall(line):
            # .lower() vì không phân biệt hoa thường
            yield word.lower(), 1

    # tổng hợp lại số lần xuất hiện của các từ trên 1 line (câu),
    # làm gọn bớt đầu vào cho reducer của step 1
    def combiner_count_words(self, word, counts):
        yield word, sum(counts)

    # nhận đầu vào sinh ra từ combiner, các cặp <key, value> của cùng 1 từ
	# sẽ về cùng một reducer và tổng hợp lại số lần xuất hiện
    def reducer_count_words(self, word, counts):
        # hàm trả về <key, values> có key=None để đưa các values=(word, num_occur) 
        # về cùng một reducer ở step 2, thuận lợi cho việc tìm số lượng xuất hiện nhiều nhất
        yield None, (word, sum(counts))

    # nhận trực tiếp đầu ra của reducer ở step 1
    def reducer_find_max_word(self, _, word_count_pairs):
        # word_count_pairs là list các tuple dạng (word, num_of_occurrences),
        # tìm max dựa trên phần tử thứ 2 của tuple
        yield max(word_count_pairs, key=lambda item: item[1])

if __name__ == '__main__':
    MRMostUsedWord.run()