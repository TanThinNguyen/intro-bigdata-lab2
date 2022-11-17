from mrjob.job import MRJob
import re

# class MRJWordCount cài đặt job đếm từ, 
# kế thừa và override lại hàm của lớp MRJob có sẵn
class MRJWordCount(MRJob):

	# hàm mapper nhận đầu vào là cặp <key, value>,
	# giá trị key ta không cần quan tâm, còn value là 
	# một dòng trong văn bản đầu vào,
	# mapper tách ra các từ xuất hiện ở mỗi câu
	def mapper(self, _, line):
		# loại bỏ khoảng trắng đầu và cuối dòng
		line = line.strip()
		# dùng regex tìm các từ chỉ gồm chữ cái, không gồm chữ số và kí tự khác
		words = re.findall(r"[a-zA-Z]+", line)
		# tạo đầu ra là các cặp <key, value> dạng <word, 1> cho mỗi từ trong 1 line
		for word in words:
			# .lower() vì không phân biệt hoa thường
			yield word.lower(), 1

	# nhận đầu vào sinh ra từ mapper,
	# tổng hợp lại số lần xuất hiện của các từ trên 1 line (câu),
	# làm gọn bớt đầu vào cho reducer
	def combiner(self, word, counts):
		yield word, sum(counts)
			
	# nhận đầu vào sinh ra từ combiner, các cặp <key, value> của cùng 1 từ
	# sẽ về cùng một reducer và tổng hợp lại số lần xuất hiện
	def reducer(self, word, counts):
		yield word, sum(counts)

if __name__ == '__main__':
	MRJWordCount.run()
