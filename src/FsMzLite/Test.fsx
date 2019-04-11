

#r @"..\FsMzLite\bin\Release\MzLite.dll"
#r @"..\FsMzLite\bin\Release\MzLite.Wiff.dll"
#r @"..\FsMzLite\bin\Release\MzLite.SQL.dll"
#r @"..\FsMzLite\bin\Release\FsMzLite.dll"
#r @"..\FsMzLite\bin\Release\Clearcore2.Muni.dll"
#r @"..\FsMzLite\bin\Release\Clearcore2.Data.dll"
#r @"..\FsMzLite\bin\Release\Clearcore2.Data.CommonInterfaces.dll"
#r @"..\FsMzLite\bin\Release\Clearcore2.Data.AnalystDataProvider.dll"
#r @"..\FsMzLite\bin\Release\Newtonsoft.Json.dll"


open System
open MzLite
open MzLite.IO
open MzLite.Model
open MzLite.Model
open MzLite.Binary
open MzLite.Commons
open MzLite.MetaData.PSIMS
open MzLite.MetaData.UO
open MzLite.Json
open MzLite.SQL
open MzLite.Wiff
open FsMzLite.AccessMassSpectrum
open FsMzLite.AccessDB


type MzLiteHelper =
    {
        RunID           : string
        MassSpectrum    : seq<MassSpectrum>
        Peaks           : seq<Peak1DArray>
        Path            : string
    }

let createMzLiteHelper (runID:string) (path:string) (spectrum:seq<MassSpectrum>) (peaks:seq<Peak1DArray>) =
    {
        MzLiteHelper.RunID          = runID
        MzLiteHelper.MassSpectrum   = spectrum
        MzLiteHelper.Peaks          = peaks
        MzLiteHelper.Path           = path
    }


let fileDir = __SOURCE_DIRECTORY__
let wiffFilePath = @"D:\Users\Patrick\Desktop\BioInformatik\MzLiteTestFiles\WiffTestFiles\20180301_MS_JT88mutID122.wiff"
let licensePath = sprintf @"%s" (fileDir + "\License\Clearcore2.license.xml")


let getWiffFileReader (path:string) =
    new WiffFileReader(path, licensePath)

let getMassSpectra (wiffFileReader:WiffFileReader) =
    wiffFileReader.Model.Runs
    |> Seq.collect (fun run -> wiffFileReader.ReadMassSpectra run.ID)

let getPeak1DArrays (wiffFileReader:WiffFileReader) =
    (getMassSpectra wiffFileReader)
    |> Seq.map (fun spectrum -> wiffFileReader.ReadSpectrumPeaks spectrum.ID)

let getMzLiteHelper (path:string) =
    let wiffFileReader = new WiffFileReader(path, licensePath)
    let runIDMassSpectra =
        wiffFileReader.Model.Runs
        |> Seq.map (fun (run) -> run.ID, wiffFileReader.ReadMassSpectra run.ID)
    runIDMassSpectra
    |> Seq.map (fun (runID, massSpectra) ->
        massSpectra
        |> Seq.map (fun spectrum -> (wiffFileReader.ReadSpectrumPeaks spectrum.ID))
        |> (fun peaks -> createMzLiteHelper runID path massSpectra peaks)
        )
    |> Seq.head

//let insertIntoDB (helper:MzLiteHelper) =
//    let mzLiteSQL = new MzLiteSQL(helper.Path + ".mzlite")
//    let bn = mzLiteSQL.BeginTransaction()
//    Seq.map2 (fun (spectrum:MassSpectrum) (peak:Peak1DArray) -> mzLiteSQL.Insert(helper.RunID, spectrum, peak)) helper.MassSpectrum helper.Peaks
//    |> Seq.length |> ignore
//    bn.Commit()
//    bn.Dispose()

//let insertIntoDB (helper:MzLiteHelper) =
//    let mzLiteSQL = new MzLiteSQL(helper.Path + ".mzlite")
//    mzLiteSQL |> ignore
//    let bn = mzLiteSQL.BeginTransaction()
//    Seq.map2 (fun (spectrum:MassSpectrum) (peak:Peak1DArray) -> mzLiteSQL.Insert(helper.RunID, spectrum, peak)) (Seq.take 1000 helper.MassSpectrum) (Seq.take 1000 helper.Peaks)
//    |> Seq.length |> ignore
//    bn.Commit()
//    bn.Dispose()

let insertIntoDB (amount:int) (helper:MzLiteHelper) =
    let mzLiteSQL = new MzLiteSQL(helper.Path + ".mzlite")
    mzLiteSQL |> ignore
    let bn = mzLiteSQL.BeginTransaction()
    Seq.map2 (fun spectrum (peak:Peak1DArray) -> mzLiteSQL.Insert(helper.RunID, spectrum, peak)) (Seq.take amount helper.MassSpectrum) (Seq.take amount helper.Peaks)
    |> Seq.length |> ignore
    bn.Commit()
    bn.Dispose()


#time
let wiffFileReader =
    getWiffFileReader wiffFilePath

let helper =
    getMzLiteHelper wiffFilePath

//let MassSpectra =
//    getMassSpectra wiffFileReader

//Seq.length MassSpectra

//let peak1DArrays =
//    getPeak1DArrays wiffFileReader

//Seq.length peak1DArrays

//let insertDB =
//    getMzLiteHelper wiffFilePath
//    |> (fun wiffFileReader -> insertIntoDB 5000 wiffFileReader)

//let helper =
//    getMzLiteHelper wiffFilePath


let tests =
    let mzLiteSQL = new MzLiteSQL(wiffFilePath + ".mzlite")
    let bn = mzLiteSQL.BeginTransaction()
    helper.MassSpectrum
    |> Seq.map (fun spectrum -> insertMSSpectrum mzLiteSQL helper.RunID wiffFileReader false spectrum)

tests
|> Seq.length

1+1
