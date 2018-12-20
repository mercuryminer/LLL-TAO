/*__________________________________________________________________________________________

            (c) Hash(BEGIN(Satoshi[2010]), END(Sunny[2012])) == Videlicet[2014] ++

            (c) Copyright The Nexus Developers 2014 - 2018

            Distributed under the MIT software license, see the accompanying
            file COPYING or http://www.opensource.org/licenses/mit-license.php.

            "ad vocem populi" - To the Voice of the People

____________________________________________________________________________________________*/

#include <LLD/include/global.h>

#include <TAO/Ledger/types/sigchain.h>

#include <Util/include/args.h>
#include <Util/include/config.h>
#include <Util/include/signals.h>
#include <Util/include/convert.h>
#include <Util/include/runtime.h>
#include <Util/include/filesystem.h>

#include <TAO/API/include/cmd.h>
#include <LLP/types/corenode.h>
#include <LLP/types/rpcnode.h>
#include <TAO/API/include/rpc.h>

#include <LLP/include/global.h>

#include <TAO/Ledger/types/mempool.h>

#include <TAO/API/include/supply.h>
#include <TAO/API/include/accounts.h>

/* Declare the Global LLD Instances. */
namespace LLD
{
    RegisterDB* regDB;
    LedgerDB*   legDB;
    LocalDB*    locDB;
}

/* Declare the Global LLP Instances. */
namespace LLP
{
    Server<TritiumNode>* TRITIUM_SERVER;
    Server<LegacyNode> * LEGACY_SERVER;
}

class TestDB : public LLD::SectorDatabase<LLD::BinaryHashMap, LLD::BinaryLRU>
{
public:
    /** The Database Constructor. To determine file location and the Bytes per Record. **/
    TestDB(const char* pszMode="r+")
    : SectorDatabase("test", pszMode) {}

    bool WriteTest(uint32_t nKey, uint32_t nValue)
    {
        return Write(std::make_pair(std::string("test"), nKey), nValue);
    }

    bool ReadTest(uint32_t nKey, uint32_t &nValue)
    {
        return Read(std::make_pair(std::string("test"), nKey), nValue);
    }

    bool GetAll(uint32_t nKey, std::vector<uint32_t>& vRecords)
    {
        std::vector<LLD::SectorKey> vKeys;

        std::pair<std::string, uint32_t> pair = std::make_pair(std::string("test"), nKey);

        DataStream ssKey(SER_LLD, LLD::DATABASE_VERSION);
        ssKey << pair;

        if(!pSectorKeys->Get(ssKey, vKeys))
            return false;

        for(auto & key : vKeys)
        {
            std::vector<uint8_t> vRecord;
            if(Get(key.vKey, vRecord))
            {
                uint32_t nRecord = 0;
                printf("%u\n", vRecord.size());
                DataStream ssData(vRecord, SER_LLD, LLD::DATABASE_VERSION);
                ssData >> nRecord;

                vRecords.push_back(nRecord);
            }
        }

        return true;
    }
};


int main(int argc, char** argv)
{
    /* Handle all the signals with signal handler method. */
    SetupSignals();


    /* Parse out the parameters */
    config::ParseParameters(argc, argv);

    TestDB test("r+");

    std::vector<uint32_t> vRecords;
    if(test.GetAll(0, vRecords))
    {
        for(auto value : vRecords)
        {
            printf("Value %u\n", value);
        }
    }


    for(uint32_t i = 0; i < 10000; i++)
    {
        //test.WriteTest(i, i); //TODO: find why id 99999 is not writing to disk

        uint32_t value;
        //test.ReadTest(i, value);

        //if(value % 10000 == 0)
        //printf("Value %u\n", value);
    }

    while(!config::fShutdown)
        runtime::sleep(1000);

    return 0;
}

int test(int argc, char** argv)
{

    /* Handle all the signals with signal handler method. */
    SetupSignals();


    /* Parse out the parameters */
    config::ParseParameters(argc, argv);


    /* Read the configuration file. */
    config::ReadConfigFile(config::mapArgs, config::mapMultiArgs);


    /* Handle Commandline switch */
    for (int i = 1; i < argc; ++i)
    {
        if (!IsSwitchChar(argv[i][0]))
        {
            if(config::GetBoolArg("-api"))
                return TAO::API::CommandLineAPI(argc, argv, i);

            return TAO::API::CommandLineRPC(argc, argv, i);
        }
    }


    /* Create directories if they don't exist yet. */
    if(!filesystem::exists(config::GetDataDir(false)) && filesystem::create_directory(config::GetDataDir(false)))
        debug::log(0, FUNCTION "Generated Path %s", __PRETTY_FUNCTION__, config::GetDataDir(false).c_str());


    /* Create the database instances. */
    LLD::regDB = new LLD::RegisterDB("r+");
    LLD::legDB = new LLD::LedgerDB("r+");
    LLD::locDB = new LLD::LocalDB("r+");


    /* Initialize the Tritium Server. */
    LLP::TRITIUM_SERVER = new LLP::Server<LLP::TritiumNode>(
        config::GetArg("-port", config::fTestNet ? 8888 : 9888),
        10,
        30,
        false,
        0,
        0,
        60,
        config::GetBoolArg("-listen", true),
        config::GetBoolArg("-meters", false),
        true);

    /* Add node to Tritium server */
    if(config::mapMultiArgs["-addnode"].size() > 0)
    {
        for(auto node : config::mapMultiArgs["-addnode"])
        {
            LLP::TRITIUM_SERVER->AddConnection(
                node,
                config::GetArg("-port", config::fTestNet ? 8888 : 9888));
        }
    }


    /* Initialize the Legacy Server. */
    LLP::LEGACY_SERVER = new LLP::Server<LLP::LegacyNode>(
        config::GetArg("-port", config::fTestNet ? 8323 : 9323),
        10,
        30,
        false,
        0,
        0,
        60,
        config::GetBoolArg("-listen", true),
        config::GetBoolArg("-meters", false),
        true);


    /* Add node to Legacy server */
    if(config::mapMultiArgs["-addnode"].size() > 0)
    {
        for(auto node : config::mapMultiArgs["-addnode"])
        {
            LLP::LEGACY_SERVER->AddConnection(
                node,
                config::GetArg("-port", config::fTestNet ? 8323 : 9323));
        }
    }

    /* Create the Core API Server. */
    LLP::Server<LLP::CoreNode>* CORE_SERVER = new LLP::Server<LLP::CoreNode>(
        config::GetArg("-apiport", 8080),
        10,
        30,
        false,
        0,
        0,
        60,
        config::GetBoolArg("-listen", true),
        config::GetBoolArg("-meters", false),
        false);

    /* Set up RPC server */
    TAO::API::RPCCommands = new TAO::API::RPC();
    TAO::API::RPCCommands->Initialize();

    TAO::API::accounts.Initialize();
    TAO::API::supply.Initialize();

    LLP::Server<LLP::RPCNode>* RPC_SERVER = new LLP::Server<LLP::RPCNode>(
        config::GetArg("-rpcport", config::fTestNet? 8336 : 9336),
        1,
        30,
        false,
        0,
        0,
        60,
        config::GetBoolArg("-listen", true),
        config::GetBoolArg("-meters", false),
        false);


    /* Busy wait for Shutdown. */
    while(!config::fShutdown)
        runtime::sleep(1000);


    /* Shutdown the servers and their subsystems */
    if(LLP::TRITIUM_SERVER)
        LLP::TRITIUM_SERVER->Shutdown();

    if(LLP::LEGACY_SERVER)
        LLP::LEGACY_SERVER->Shutdown();

    if(CORE_SERVER)
        CORE_SERVER->Shutdown();

    if(RPC_SERVER)
        RPC_SERVER->Shutdown();

    return 0;
}
