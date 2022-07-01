#include <algorithm>
#include <chrono>
#include <fstream>
#include <iostream>
#include <mutex>
#include <queue>
#include <string>
#include <vector>
#include <array>
#include <sstream>

constexpr unsigned undefined_uint = std::numeric_limits<unsigned>::max();
typedef std::pair<size_t, size_t> WordBounds;
typedef std::vector<WordBounds> WordBoundsVec;

static const std::string inputFilename = "./../../../input.txt";
static const std::string outputFilename = "./../../../output.txt";

// for single-threaded test
static constexpr size_t wordCountExpected = 60000000;
static constexpr size_t wordsPerLineBufferLen = 30;

// for multi-threaded test
static constexpr size_t stringsArrBufSize = 100;
static constexpr size_t workersCount = 8;
static const std::string theLastStringEver = "}";

WordBoundsVec parseLine(const char* pos, const size_t size)
{
	size_t currLetterStart = undefined_uint;
	WordBoundsVec words;
	words.reserve(wordsPerLineBufferLen);
	
	for (size_t cursor = 0; cursor < size; ++cursor)
	{
		const char currChar = pos[cursor];
		const bool isLetter = currChar >= 0 && isalpha(currChar);
		const bool wasReadingWord = currLetterStart != undefined_uint;
		const bool isLastSymbol = cursor == size - 1;

		if (isLetter)
		{
			if (wasReadingWord && !isLastSymbol)
				continue;
				// continuing reading current word

			if (!wasReadingWord && isLastSymbol)
			{
				// one-letter-word at the end of the line
				words.emplace_back(cursor, 1);
				break;
			}

			if (wasReadingWord && isLastSymbol)
			{
				// last letter of word & of last symbol of line
				words.emplace_back(currLetterStart, cursor - currLetterStart + 1);
				break;
			}

			if (!wasReadingWord)
			{
				// word started
				currLetterStart = cursor;
			}
		}
		else if (wasReadingWord)
		{
			// word ended, now space or something else
			words.emplace_back(currLetterStart, cursor - currLetterStart);
			currLetterStart = undefined_uint;
		}
	}
	words.shrink_to_fit();
	return words;
}

class WordFreqSimpleHelper
{
public:
	static void process()
	{
		std::cout << "SINGLE-THREADED TEST STARTED" << std::endl;

		const auto allTime0 = std::chrono::system_clock::now();
		const auto readTime0 = allTime0;

		std::ifstream infile(inputFilename);
		std::stringstream buffer;
		buffer << infile.rdbuf();

		const auto readTime1 = std::chrono::system_clock::now();
		const auto readTime = std::chrono::duration_cast<std::chrono::milliseconds>(readTime1 - readTime0);
		std::cout << "Read complete in " << readTime << std::endl;

		std::vector<std::string> words;
		std::string line;
		words.reserve(wordCountExpected);

		while (std::getline(buffer, line))
		{
			const auto& wordsInLine = parseLine(line.data(), line.size());
			for (const auto& [startPos, wordSize] : wordsInLine)
			{
				words.emplace_back(&line[startPos], wordSize);
				std::ranges::transform(words.back(), words.back().begin(), [](const unsigned char c) { return std::tolower(c); });
			}
		}

		const auto parseTime1 = std::chrono::system_clock::now();
		const auto parseTime = std::chrono::duration_cast<std::chrono::milliseconds>(parseTime1 - readTime1);
		std::cout << "words (" << words.size() << ") parse complete in " << parseTime << std::endl;

		std::ranges::sort(words);
		const auto sortTime1 = std::chrono::system_clock::now();
		const auto sortTime = std::chrono::duration_cast<std::chrono::milliseconds>(sortTime1 - parseTime1);
		std::cout << "words sort complete in " << sortTime << std::endl;

		std::vector<std::pair<std::string_view, unsigned> > wordCount;
		std::string_view lastWordView;
		for (const auto& word : words)
		{
			std::string_view currWordView = word;
			if (currWordView != lastWordView)
			{
				wordCount.emplace_back(currWordView, 1);
				lastWordView = currWordView;
			}
			else
				wordCount.back().second++;
		}

		const auto countTime1 = std::chrono::system_clock::now();
		const auto countTime = std::chrono::duration_cast<std::chrono::milliseconds>(countTime1 - sortTime1);
		std::cout << "count complete in " << countTime << std::endl;

		std::ranges::stable_sort(wordCount, [](const decltype(wordCount)::value_type& wordCountPair1, const decltype(wordCount)::value_type& wordCountPair2) {return wordCountPair1.second > wordCountPair2.second; });

		const auto resortTime1 = std::chrono::system_clock::now();
		const auto resortTime = std::chrono::duration_cast<std::chrono::milliseconds>(resortTime1 - countTime1);
		std::cout << "re-sort complete in " << resortTime << std::endl;

		std::ofstream outputFile(outputFilename);
		for (const auto& wordPair : wordCount)
			outputFile << wordPair.second << " " << wordPair.first << "\n";

		const auto saveTime1 = std::chrono::system_clock::now();
		const auto saveTime = std::chrono::duration_cast<std::chrono::milliseconds>(saveTime1 - resortTime1);
		std::cout << "save complete in " << saveTime << std::endl;

		const auto allTime1 = std::chrono::system_clock::now();
		const auto allTime = std::chrono::duration_cast<std::chrono::milliseconds>(allTime1 - allTime0);
		std::cout << "all complete in " << allTime << std::endl;
	}
};

class WordFreqAsyncWorker
{
public:

	typedef std::shared_ptr<WordFreqAsyncWorker> Ptr;
	struct StringsBuffer
	{		
		typedef std::array<std::string, stringsArrBufSize> StringsArr;

		StringsArr arr;
		typedef std::shared_ptr<StringsBuffer> Ptr;
	};

	bool AddLinesToProcess(const StringsBuffer::Ptr& stringsBuf)
	{
		if (queueMutex.try_lock())
		{
			// luckily the queue is not busy, let's add new strings now

			linesToProcess.push(stringsBuf);
			queueMutex.unlock();
			queueCond.notify_one();
			return true;
		}

		return false;
		// queue is busy, try again later, don't block source thread
	}

	void Start()
	{
		thread = std::thread([this] {process(); });
	}

	void RequestStop()
	{
		// source thread notifies that there won't be any new data added to worker queue
		stopRequested = true;
		queueCond.notify_one();
	}

	void Join()
	{
		if (thread.joinable())
			thread.join();
	}

	// just for pretty output
	void SetId(const unsigned i) { id = i; }

	const std::vector<std::pair<std::string, unsigned> >& GetWordCount() const {return wordCount;}

private:
	std::queue<StringsBuffer::Ptr> linesToProcess;
	std::vector<std::pair<std::string, unsigned> > wordCount;
	std::mutex queueMutex;
	std::atomic<bool> stopRequested = false;
	std::thread thread;
	std::condition_variable queueCond;
	unsigned id = 0;

	void process()
	{
		std::vector<std::string> words;
		constexpr size_t wordsPerThreadExpected = 10000000;
		words.reserve(wordsPerThreadExpected);

		while (true)
		{
			StringsBuffer::Ptr stringsBuffer;
			volatile size_t linesRemain = 0;

			auto getNextBuffer = [this, &stringsBuffer, &linesRemain]()
			{
				stringsBuffer = linesToProcess.front();
				linesRemain = linesToProcess.size();
				linesToProcess.pop();				
			};

			if (stopRequested && !linesToProcess.empty())
			{
				// no need locks, as source thread stopped sending buffers
				getNextBuffer();
			}
			else {

				std::unique_lock queueLock(queueMutex);
				if (!linesToProcess.empty())
				{
					getNextBuffer();
				}
				else if (stopRequested)
					break;
				else
				{
					queueCond.wait(queueLock);
					continue;
				}
			}

			for (size_t i = 0; i < stringsArrBufSize; ++i)
			{
				const std::string& line = stringsBuffer->arr[i];
				if (line.empty())
					continue;

				const auto& wordsInLine = parseLine(line.data(), line.size());
				for (const auto& [startPos, wordSize] : wordsInLine)
				{
					words.emplace_back(&line[startPos], wordSize);
					std::ranges::transform(words.back(), words.back().begin(), [](const unsigned char c) { return std::tolower(c); });
				}
			}
		}

		words.shrink_to_fit();

		std::cout << "worker " << id << " finished parsing " << words.size() << " words" << std::endl;

		const auto sortTime0 = std::chrono::system_clock::now();
		std::ranges::sort(words);
		const auto sortTime1 = std::chrono::system_clock::now();
		const auto sortTime = std::chrono::duration_cast<std::chrono::milliseconds>(sortTime1 - sortTime0);
		std::cout << "worker " << id << " words sort complete in " << sortTime << std::endl;

		const auto countTime0 = std::chrono::system_clock::now();

		std::string lastWordView;
		for (const auto& word : words)
		{
			std::string_view currWordView = word;
			if (currWordView != lastWordView)
			{
				wordCount.emplace_back(currWordView, 1);
				lastWordView = currWordView;
			}
			else
				wordCount.back().second++;
		}

		const auto countTime1 = std::chrono::system_clock::now();
		const auto countTime = std::chrono::duration_cast<std::chrono::milliseconds>(countTime1 - countTime0);
		std::cout << "worker " << id << " count complete in " << countTime << std::endl;
	}
};

class WordFreqAsyncHelper
{
public:
	static void process()
	{
		std::cout << "MULTITHREADED TEST STARTED" << std::endl;

		const auto allTime0 = std::chrono::system_clock::now();

		std::vector<WordFreqAsyncWorker::Ptr> workers;
		for (size_t i = 0; i < workersCount; ++i) {

			auto worker = std::make_shared<WordFreqAsyncWorker>();
			worker->SetId(static_cast<unsigned>(i));
			worker->Start();
			workers.push_back(worker);
		}

		// PHASE 1: reading lines and feeding workers with data chunks

		std::ifstream infile(inputFilename);
		const auto readTime0 = std::chrono::system_clock::now();
		{
			size_t workerInd = 0;

			auto stringsBuffer = std::make_shared<WordFreqAsyncWorker::StringsBuffer>();
			size_t stringsBufInd = 0;

			auto sendToNextWorker = [&workerInd, &workers](const WordFreqAsyncWorker::StringsBuffer::Ptr& stringsBuf)
			{
				bool added = false;
				while (!added)
				{
					added = workers[workerInd]->AddLinesToProcess(stringsBuf);
					workerInd = (workerInd + 1) % workersCount;

					// iterating through workers, searching for a queue that isn't busy to add new data for processing by worker.
				}
			};

			std::string line;
			while (std::getline(infile, line))
			{
				if (line.empty())
					continue;

				if (stringsBufInd < stringsBuffer->arr.size())
				{
					stringsBuffer->arr[stringsBufInd] = std::move(line);
					stringsBufInd++;
				}

				if (stringsBufInd == stringsBuffer->arr.size())
				{
					// data chunk is full and ready
					sendToNextWorker(stringsBuffer);

					stringsBuffer = std::make_shared<WordFreqAsyncWorker::StringsBuffer>();
					stringsBufInd = 0;
				}
			}

			// sending last chunk, unfilled
			sendToNextWorker(stringsBuffer);
		}

		const auto readTime1 = std::chrono::system_clock::now();
		const auto readTime = std::chrono::duration_cast<std::chrono::milliseconds>(readTime1 - readTime0);
		std::cout << "Read complete in " << readTime << std::endl;


		// telling workers that there won't be any new data incoming
		for (size_t i = 0; i < workersCount; ++i) {
			workers[i]->RequestStop();
		}

		for (size_t i = 0; i < workersCount; ++i) {
			workers[i]->Join();
		}

		// PHASE 2: (single-threaded) merge word counts from all workers

		const auto countMergeTime0 = std::chrono::system_clock::now();

		std::vector<std::pair<std::string, unsigned> > wordCount;
		auto findIndicesWithLowestAl = [&workers](const std::vector<size_t>& indices)
		{
			// returns indexes of workers which have the same most 'alphabetically-low' word at their current cursor positions

			std::vector<size_t> result;			
			std::string_view lowestAlStr = theLastStringEver;

			for (size_t workerInd = 0; workerInd < workers.size(); workerInd++)
			{
				const auto& wordCountLocal = workers[workerInd]->GetWordCount();

				if (indices[workerInd] >= wordCountLocal.size())
					continue;
					// this worker is already out of words

				const std::string_view checkStr = wordCountLocal[indices[workerInd]].first;
				if (checkStr < lowestAlStr) {
					result.clear();
					lowestAlStr = checkStr;
				}

				if (checkStr == lowestAlStr)
					result.push_back(workerInd);
			}

			return result;
		};

		std::vector<size_t> cursorPerWorker;
		for (size_t indInd = 0; indInd < workers.size(); ++indInd)
			cursorPerWorker.push_back(0);

		while(true)
		{
			const auto workerIndexesWithLowestAl = findIndicesWithLowestAl(cursorPerWorker);
			if (workerIndexesWithLowestAl.empty())
				break;
				// no more words in workers internal dictionaries

			{
				// adding new word into final dictionary. Searching for 'alphabetically-lowest' word in workers, and get 0-th one as a source for word string itself and initial count

				const auto firstWorkerWithLowestAlInd = workerIndexesWithLowestAl[0];
				const auto& [word, count] = workers[firstWorkerWithLowestAlInd]->GetWordCount()[cursorPerWorker[firstWorkerWithLowestAlInd]];
				wordCount.emplace_back(word, count);
				cursorPerWorker[firstWorkerWithLowestAlInd]++;
			}

			for (size_t otherWorkerInd = 1; otherWorkerInd < workerIndexesWithLowestAl.size(); otherWorkerInd++)
			{
				// adding count for all other workers that has this word

				wordCount.back().second += workers[workerIndexesWithLowestAl[otherWorkerInd]]->GetWordCount()[cursorPerWorker[workerIndexesWithLowestAl[otherWorkerInd]]].second;
				cursorPerWorker[workerIndexesWithLowestAl[otherWorkerInd]]++;
			}
		}

		workers.clear();

		const auto countMergeTime1 = std::chrono::system_clock::now();
		const auto countMergeTime = std::chrono::duration_cast<std::chrono::milliseconds>(countMergeTime1 - countMergeTime0);
		std::cout << "count merge complete in " << countMergeTime << std::endl;

		// PHASE 3: sort merged dictionaries by word count

		std::ranges::stable_sort(wordCount, [](const decltype(wordCount)::value_type& wordCountPair1, const decltype(wordCount)::value_type& wordCountPair2) {return wordCountPair1.second > wordCountPair2.second; });

		const auto resortTime1 = std::chrono::system_clock::now();
		const auto resortTime = std::chrono::duration_cast<std::chrono::milliseconds>(resortTime1 - countMergeTime1);
		std::cout << "re-sort complete in " << resortTime << std::endl;

		std::ofstream outputFile(outputFilename);
		for (const auto& wordPair : wordCount)
			outputFile << wordPair.second << " " << wordPair.first << "\n";

		const auto saveTime1 = std::chrono::system_clock::now();
		const auto saveTime = std::chrono::duration_cast<std::chrono::milliseconds>(saveTime1 - resortTime1);
		std::cout << "save complete in " << saveTime << std::endl;

		const auto allTime1 = std::chrono::system_clock::now();
		const auto allTime = std::chrono::duration_cast<std::chrono::milliseconds>(allTime1 - allTime0);
		std::cout << "all complete in " << allTime << std::endl;
	}
};

int main()
{
	// single-threaded test
	//WordFreqSimpleHelper::process();

	// multi-threaded test
	WordFreqAsyncHelper::process();

	return 0;
}
