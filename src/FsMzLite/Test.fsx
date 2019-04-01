

#r @"..\FsMzLite\bin\Debug\MzLite.dll"
#r @"..\FsMzLite\bin\Debug\MzLite.Wiff.dll"
#r @"..\FsMzLite\bin\Debug\MzLite.SQL.dll"
#r @"..\FsMzLite\bin\Debug\FsMzLite.dll"
#r @"..\FsMzLite\bin\Debug\Clearcore2.Muni.dll"
#r @"..\FsMzLite\bin\Debug\Clearcore2.Data.dll"
#r @"..\FsMzLite\bin\Debug\Clearcore2.Data.CommonInterfaces.dll"
#r @"..\FsMzLite\bin\Debug\Clearcore2.Data.AnalystDataProvider.dll"
#r @"..\FsMzLite\bin\Debug\Newtonsoft.Json.dll"


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
let wiffFilePath = @"C:\Users\Student\source\repos\wiffTestFiles\20171129 FW LWagg001.wiff"
let licensePath = sprintf @"%s" (fileDir + "\License\Clearcore2.license.xml")

let wiffObject = new WiffFileReader(wiffFilePath, licensePath)

#time
let massSpectra = 
    wiffObject.Model.Runs
    |> Seq.collect (fun run -> getMassSpectraBy wiffObject run.ID)

let getPeak1DArrays (wiffFileReader:WiffFileReader) =
    massSpectra
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

let insertIntoDB (helper:MzLiteHelper) =
    let mzLiteSQL = new MzLiteSQL(helper.Path + ".mzlite")
    mzLiteSQL |> ignore
    let bn = mzLiteSQL.BeginTransaction()
    Seq.map2 (fun (spectrum:MassSpectrum) (peak:Peak1DArray) -> mzLiteSQL.Insert(helper.RunID, spectrum, peak)) (Seq.take 1000 helper.MassSpectrum) (Seq.take 1000 helper.Peaks)
    |> Seq.length |> ignore
    bn.Commit()
    bn.Dispose()


let peak1DArrays = getPeak1DArrays wiffObject

//Seq.length peak1DArrays

let insertDB =
    getMzLiteHelper wiffFilePath
    |> (fun wiffFileReader -> insertIntoDB wiffFileReader)

//let helper =
//    getMzLiteHelper wiffFilePath

//Seq.item 0 helper.Peaks