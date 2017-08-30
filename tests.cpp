#include <windows.h>
#include <Shlobj.h>
#include <Shlwapi.h>

#include <iostream>
#include <string>
#include <sstream>
#include <algorithm>
#include <fstream>
#include <functional>
#include <memory>
#include <chrono>

namespace IO
{
    DWORD SectorSize{ 0 };

    void Init()
    {
        DWORD dw1, dw2, dw3;
        GetDiskFreeSpace(nullptr, &dw1, &SectorSize, &dw2, &dw3);
    }

#define IO_SECTOR_ALIGN(val) (((val)+IO::SectorSize -1) & ~(IO::SectorSize-1))
#define IS_IO_SECTOR_ALIGNED(val) ((uintptr_t(val) % IO::SectorSize)==0)

#define IO_START_CLOCK() auto _then_ = std::chrono::high_resolution_clock::now()
#define IO_STOP_CLOCK() dur = (static_cast<double>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now() - _then_).count()))

    void* AllocIOAligned(size_t len)
    {
        std::size_t space = len + size_t(SectorSize - 1);
        auto ptr = malloc(space + sizeof(void*));		
        auto original_ptr = ptr;

        auto ptr_bytes = static_cast<char*>(ptr);
        ptr_bytes += sizeof(void*);
        ptr = static_cast<void*>(ptr_bytes);

        ptr = std::align(SectorSize, len, ptr, space);

        // store pointer to block before aligned pointer
        ptr_bytes = static_cast<char*>(ptr);
        ptr_bytes -= sizeof(void*);
        memcpy(ptr_bytes, &original_ptr, sizeof(void*));

        return ptr;
    }

    void FreeIOAligned(void* aligned)
    {
        if (aligned)
        {
            free(static_cast<void**>(aligned)[-1]);
        }
    }

    double WriteFileFStream(const char* name, void* data, size_t len)
    {
        double dur = 0;
        std::fstream fs;
        fs.open(name, fs.binary | fs.out | fs.trunc);
        if (fs.is_open())
        {            
            IO_START_CLOCK();
            fs.write(reinterpret_cast<const char*>(data), len);
            IO_STOP_CLOCK();            
        }
        return dur;
    }

    double WriteFileWin32NoCachedNoOverlapped(const char* name, void* data, size_t len)
    {
        double dur = 0;
        auto handle = CreateFileA(reinterpret_cast<LPCSTR>(name), GENERIC_WRITE, FILE_SHARE_WRITE,
                                nullptr, CREATE_ALWAYS, FILE_FLAG_NO_BUFFERING, nullptr);
        if (handle)
        {
            DWORD alignedReq = IO_SECTOR_ALIGN(static_cast<DWORD>(len));
            // need an aligned buffer
            auto aligned = AllocIOAligned(alignedReq);
            memcpy(aligned, data, len);
            {
                IO_START_CLOCK();
                DWORD written;
                WriteFile(handle, aligned, alignedReq, reinterpret_cast<LPDWORD>(&written), nullptr);
                IO_STOP_CLOCK();
            }
            FreeIOAligned(aligned);
            CloseHandle(handle);
        }

        return dur;
    }

    //ZZZ:
    OVERLAPPED OverlappedPool[2048];
    size_t NextFreeOverlapped{ 0 };

    double WriteFileWin32NoCachedOverlapped(const char* name, void* data, size_t len)
    {
        double dur = 0;
        auto handle = CreateFileA(reinterpret_cast<LPCSTR>(name), GENERIC_WRITE, FILE_SHARE_WRITE,
            nullptr, CREATE_ALWAYS, FILE_FLAG_NO_BUFFERING | FILE_FLAG_OVERLAPPED, nullptr);
        if (handle)
        {
            // need an aligned buffer
            DWORD alignedReq = IO_SECTOR_ALIGN(static_cast<DWORD>(len));
            auto aligned = AllocIOAligned(alignedReq);
            memcpy(aligned, data, len);			
            {
                IO_START_CLOCK();
                DWORD written;
                memset(&OverlappedPool[NextFreeOverlapped], 0, sizeof(OVERLAPPED));
                WriteFile(handle, aligned, alignedReq, reinterpret_cast<LPDWORD>(&written), &OverlappedPool[NextFreeOverlapped++]);
                IO_STOP_CLOCK();
            }
            FreeIOAligned(aligned);
            CloseHandle(handle);
        }

        return dur;
    }

    double WriteFileWin32CachedOverlapped(const char* name, void* data, size_t len)
    {
        double dur = 0;
        auto handle = CreateFileA(reinterpret_cast<LPCSTR>(name), GENERIC_WRITE, FILE_SHARE_WRITE,
            nullptr, CREATE_ALWAYS, FILE_FLAG_OVERLAPPED, nullptr);
        if (handle)
        {            
            IO_START_CLOCK();
            DWORD written;
            memset(&OverlappedPool[NextFreeOverlapped], 0, sizeof(OVERLAPPED));
            WriteFile(handle, data, len, reinterpret_cast<LPDWORD>(&written), &OverlappedPool[NextFreeOverlapped++]);
            IO_STOP_CLOCK();
            
            CloseHandle(handle);
        }

        return dur;
    }

    double WriteFileWin32CachedNonOverlapped(const char* name, void* data, size_t len)
    {
        double dur = 0;
        auto handle = CreateFileA(reinterpret_cast<LPCSTR>(name), GENERIC_WRITE, FILE_SHARE_WRITE,
            nullptr, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
        if (handle)
        {
            IO_START_CLOCK();
            DWORD written;
            WriteFile(handle, data, len, reinterpret_cast<LPDWORD>(&written), nullptr);
            IO_STOP_CLOCK();
            
            CloseHandle(handle);
        }

        return dur;
    }
}

namespace Tests
{
    constexpr int FOUR_K_BLOCKS{ 512 };
    constexpr int FOUR_K{ 4096 };
    constexpr int MAX_BUFFER_SIZE{ FOUR_K_BLOCKS * FOUR_K };
    constexpr int FILE_WRITE_COUNT{ 1024 };
    char	Buffer[MAX_BUFFER_SIZE];
    double DurationNs{ 0 };
    char DataPath[MAX_PATH];
    int TestRound{ 0 };

    struct sample
    {
        uint32_t    _bufferLength;
        double      _nsTime;
    };

    sample* RunSamples = nullptr;
    int     Sample;

    bool InitNextTest()
    {
        IO::NextFreeOverlapped = 0;
        if (RunSamples)
            delete[] RunSamples;
        RunSamples = new sample[FILE_WRITE_COUNT];
        Sample = 0;
        
        if (SUCCEEDED(SHGetFolderPathA(NULL,
            CSIDL_MYDOCUMENTS,
            NULL,
            0,
            DataPath)))
        {
            CHAR buff[_MAX_PATH];
            sprintf_s(buff, "fileio_tests_%d", ++TestRound);
            PathAppendA(DataPath, buff);
            auto res = CreateDirectoryA(DataPath, nullptr);
            return SUCCEEDED(res) || res == ERROR_ALREADY_EXISTS;
        }
        return false;
    }

    bool InitTests()
    {
        IO::Init();
        TestRound = 0;
        return true;
    }

    std::string CreateRandomFileName()
    {
        std::stringstream ss;
        ss << DataPath << "\\" << rand() << "_" << rand() << ".dat";
        return ss.str();
    }

    size_t AllocData()
    {
        auto block = std::max<size_t>(rand() % FOUR_K_BLOCKS, 2);
        return block * FOUR_K;
    }

    void WriteFilesTest(std::function<double(const char*, void*, size_t)> writeOne)
    {		
        size_t bytesWritten = 0;
        double acc = 0;
        for (int n = 0; n < FILE_WRITE_COUNT; ++n)
        {
            auto size = AllocData();
            bytesWritten += size;
            auto name = CreateRandomFileName();
            
            DurationNs = writeOne(name.c_str(), Buffer, size);

            RunSamples[Sample]._bufferLength = size;
            RunSamples[Sample]._nsTime = DurationNs;
            Sample++;

            acc += DurationNs;
        }
        DurationNs = acc / FILE_WRITE_COUNT;
        auto fduration = acc / FILE_WRITE_COUNT;
        std::cout << FILE_WRITE_COUNT << " file writes, a total of " << bytesWritten << " bytes, took " << fduration*0.00001 << " ms per file on average" << std::endl;
    }

    void WriteSamplesForRun(const std::string & runName)
    {
        std::fstream fs;
        std::stringstream ss;
        ss << DataPath << "\\" << runName << ".tsv";
        fs.open(ss.str().c_str(), fs.out | fs.trunc);
        if (fs.is_open())
        {
            ss = std::stringstream();

            // column headers
            for (auto run = 0; run < FILE_WRITE_COUNT; ++run)
                ss << run << "\t";
            fs << ss.rdbuf() << std::endl;

            // rows
            ss = std::stringstream();
            for (auto sample = 0; sample < Sample; ++sample)
                ss << RunSamples[sample]._bufferLength << "\t";
            fs << ss.rdbuf() << std::endl;
            ss = std::stringstream();
            for (auto sample = 0; sample < Sample; ++sample)
                ss << RunSamples[sample]._nsTime << "\t";
            fs << ss.rdbuf() << std::endl;
        }
    }

    void Run(const std::string & runName, std::function<double(const char*, void*, size_t)> writeOne)
    {
        if (!InitNextTest())
        {
            std::cerr << "failed to set up test\n";
            return;
        }
        std::cout << runName << ":" << std::endl;
        WriteFilesTest(writeOne);
        WriteSamplesForRun(runName);
    }
}

int main()
{
    Tests::InitTests();
    Tests::Run("FStream", IO::WriteFileFStream);	
    Tests::Run("CachedNonOverlapped", IO::WriteFileWin32CachedNonOverlapped);
    Tests::Run("CachedOverlapped", IO::WriteFileWin32CachedOverlapped);
    Tests::Run("UncachedNonOverlapped", IO::WriteFileWin32NoCachedNoOverlapped);
    Tests::Run("UncachedOverlapped", IO::WriteFileWin32NoCachedOverlapped);
    return 0;
}

