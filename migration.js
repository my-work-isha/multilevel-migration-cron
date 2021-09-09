var express = require('express');
const cfenv = require('cfenv');
const ibmdb = require('ibm_db');
const CronJob = require('cron').CronJob;
const _ = require('lodash');
const Cloudant = require('cloudant');


var keylist = [];

//  reviewform 
/*var modelfields = ["Form", "txGeo", "rtxReviewFindings", "rtxReviewFindings_1", "dlgReviews", "rtReviewlinks", "rtxRec", "rtxRec_1", "rtxRevRec", "rtxRevRec_1", "dtAIRevStartDate", "dspAIRevStartDate", "rtActionitems", "rtActionitems_1", "kwStaffMember_1", "txStaffMermberEmail", "kwStatus", "kwStatus_dsp", "dtACRBDts", "txReviewLengthDays",
    "dtFinalBPCRB", "kwDecision", "dtACRBTermDt", "txTermReason", "TSDNumSamples", "TSDNumSamplesNDADec", "TSDNumRTMReviews", "TSDNumSamplesEU", "TSDGrossRecovery", "TSDMAMAmounts", "TSDBoardWaivers", "TSDNetRecovery", "BGFFindings", "BGFCFMRecovery", "BRET_Score", "Fail_Conditions", "Confirm_Status", "TSDNumSamples_2", "TSDNumSamplesNDADec_2",
    "TSDNumRTMReviews_2", "TSDNumSamplesEU_2", "TSDGrossRecovery_2", "TSDMAMAmounts_2", "TSDBoardWaivers_2", "TSDNetRecovery_2", "BGFFinding_2", "BGFCFMRecovery_2", "BRET_Score_2", "Fail_Conditions_2", "Confirm_Status_2", "txRecSummary", "nmTotDollarAmt", "nmTotDollarAmt_dsp", "nmTotDollarAmtLA", "dtExecLtr", "nmFinalRecvAmt", "Recovery_Amount_text",
    "nmFinalRecvAmt_dsp", "Recovery_Amount_text_dsp", "nmFinalRecvAmtLA", "Recovery_Amount_textLA", "nm10perdiscount", "dtPayDue_1", "txInvoiceNo_1", "dtRecovInv_1", "dtBalPaid_1", "txtLocalCurTypeRate_1", "nmInvoiceAmtDollars_1", "nmDaysOS_1", "txtInvoiceComments_1", "dtPayDue", "txInvoiceNo", "dtRecovInv", "dtBalPaid", "txtLocalCurTypeRate",
    "dtInvoice_", "dtInvoice", "dtRecentInvoicepaid", "nmInvoiceAmtDollars", "nmDaysOS", "txComments2", "txPriLog1", "dtPriDt1", "txPriAlleg1", "txPriDecis1", "txPriRecv1", "txPriChann1", "txPriRevRecv1", "txPriInfraction", "txPriStaff1", "rtAllegAttachments", "txLegalName_1", "txAddress1", "txAddress2", "txCity", "txState", "txZip", "kwCountryCode",
    "kwPartnerRel", "txDBAName", "txTelNum", "txFaxNum", "txWebsite", "kwDistributor", "txDistAddr1", "txDistAddr2", "txDistCity", "txDistState", "txDistZip", "txExecName", "txExecTitle", "txExecName_1", "txExecTitle_1", "txExecName_2", "txExecTitle_2", "txExecPhone", "txExecPhone_1", "txExecPhone_2", "txExecEmail", "txExecEmail_1", "txExecEmail_2",
    "txContName1", "txContName1_1", "txContName1_2", "txContName1_3", "txContName1_4", "txContTtl1", "txContTtl1_1", "txContTtl1_2", "txContTtl1_3", "txContTtl1_4", "txContTel1", "txContTel1_1", "txContTel1_2", "txContTel1_3", "txContTel1_4", "txContFax1", "txContFax1_1", "txContFax1_2", "txContFax1_3", "txContFax1_4", "txContEmail1", "txContEmail1_1",
    "txContEmail1_2", "txContEmail1_3", "txContEmail1_4", "txAgreeNum1", "txAgreeNum1_1", "txAgreeNum1_2", "txAgreeNum1_3", "txAgreeNum1_4", "txAgreeNum1_5", "txAgreeNum1_6", "txAgreeNum1_7", "txAgreeNum1_8", "txAgreeNum1_9", "kwChannel_1", "kwChannel_1_1", "kwChannel_1_2", "kwChannel_1_3", "kwChannel_1_4", "kwChannel_1_5", "kwChannel_1_6",
    "kwChannel_1_7", "kwChannel_1_8", "kwChannel_1_9", "dtOrigDt1", "dtOrigDt1_1", "dtOrigDt1_2", "dtOrigDt1_3", "dtOrigDt1_4", "dtOrigDt1_5", "dtOrigDt1_6", "dtOrigDt1_7", "dtOrigDt1_8", "dtOrigDt1_9", "dtStartDt1", "dtStartDt1_1", "dtStartDt1_2", "dtStartDt1_3", "dtStartDt1_4", "dtStartDt1_5", "dtStartDt1_6", "dtStartDt1_7", "dtStartDt1_8",
    "dtStartDt1_9", "dtEndDt1", "dtEndDt1_1", "dtEndDt1_2", "dtEndDt1_3", "dtEndDt1_4", "dtEndDt1_5", "dtEndDt1_6", "dtEndDt1_7", "dtEndDt1_8", "dtEndDt1_9", "txRel", "txRel_1", "txRel_2", "txRel_3", "txRel_4", "txRel_5", "txRel_6", "txRel_7", "txRel_8", "txRel_9", "txEntNum", "txGeoID", "txCustNum", "txSAPNum", "txAgreementProg", "txAgreementProg_1",
    "txAgreementProg_2", "txAgreementProg_3", "txAgreementProg_4", "txAgreementProg_5", "txAgreeNo", "txAgreeNo_1", "txAgreeNo_2", "txAgreeNo_3", "txAgreeNo_4", "txAgreeNo_5", "dtAgrOrigDt", "dtAgrOrigDt_1", "dtAgrOrigDt_2", "dtAgrOrigDt_3", "dtAgrOrigDt_4", "dtAgrOrigDt_5", "txAgrStDt", "txAgrStDt_1", "txAgrStDt_2", "txAgrStDt_3", "txAgrStDt_4",
    "txAgrStDt_5", "txAgrEndDt", "txAgrEndDt_1", "txAgrEndDt_2", "txAgrEndDt_3", "txAgrEndDt_4", "txAgrEndDt_5", "dspAgreeNo", "dspAgrStDt", "dspCountryCode", "txLOCID", "txCurYTD", "txPrevRevYTD", "kwAuthProds", "dspAgrOrigDt", "dspAgrEndDt", "kwswgRegion", "kwswgBrand", "KwswgProducts", "signature", "sigtimestamp", "rdMeasurementCr", "dstReview",
    "rdAuditFirmRvw", "kwAuditFirm", "dstScope", "kwPreRevStatus", "txPreRevComments", "txtCurprimname", "txCountry", "txGrowthMarketReg", "txMajorMarket", "txGrowthMarket", "kwChannel", "txLogNo", "dtOrigDate", "dspOrigDate", "kwInvestType", "txInvestTypeExplain", "kwMethodReview", "kwInvestigation", "rdReProactive", "txLegalName", "ReviewFlag",
    "kwStaffMember", "dspStaffMmbr", "nmAddEditor", "dspAddEditor", "dtMCDate", "dtMCRDate", "dtReviewDt", "nmDaysRevOpen", "nmDaysMCOpen", "nmDaysDocsOS", "kwPlanYear", "kwPlanQuarter", "dtTargetAnnounce_Chk3", "dtAnnounce_Chk3", "txtUserAnnounce_Chk3", "dtUserAnnounce_Chk3", "txtComments_Chk3", "dspAnnounce_Chk3", "dtKickCall_Chk4",
    "dspKickCall_Chk4", "txtUserKickCall_Chk4", "dtUserKickCall_Chk4", "txtComments_Chk4", "txtAppli", "dtDataReq_Chk5", "dspDataReq_Chk5", "txtUserDataReq_Chk5", "dtUserDataReq_Chk5", "txtComments_Chk5", "dtPartnerRes_Chk6", "dspPartnerRes_Chk6", "txtUserPartnerRes_Chk6", "dtUserPartnerRes_Chk6", "txtComments_Chk6", "dtPlannedRvwDt",
    "dtReview_Chk7", "txtUserReview_Chk7", "dtUserReview_Chk7", "txtComments_Chk7", "dtEstConducted_Chk8", "dspIntialFind_Chk8", "dtIntialFind_Chk8", "txtUserIntialFind_Chk8", "dtUserIntialFind_Chk8", "txtComments_Chk8", "dtFinalPartnerRes_Chk9", "dspFinalPartnerRes_Chk9", "txtUserFinalPartnerRes_Chk9", "dtUserFinalPartnerRes_Chk9",
    "txtComments_Chk9", "dtMgmtAppr_Chk10", "txtUserMgmtAppr_Chk10", "dtUserMgmtAppr_Chk10", "txtComments_Chk10", "dtTargetACRB", "dtBPCRB_Chk11", "txtUserBPCRB_Chk11", "dtUserBPCRB_Chk11", "txtComments_Chk11", "dtDecision_Chk12", "txtUserDecision_Chk12", "dtUserDecision_Chk12", "txtComments_Chk12", "dtCertify_Chk12a", "dspCertify_Chk12a",
    "txtUserCertifyDate_Chk12a", "dtUserCertifyDate_Chk12a", "txtComments_Chk12a", "dtClose_Chk13", "dspClose_Chk13", "txtUserClose_Chk13", "dtUserClose_Chk13", "txtComments_Chk13", "dtRDLFiled_Chk14", "dspRDLFiled_Chk14", "txtRDL_Chk14", "dtRDL_Chk14", "dlgRDLStatus", "txtComments_Chk14", "kwChanne", "AllegActionTaken", "ViolationObserv",
    "dtAllegation", "nmMonAllegDt", "dtDistributeStaffDt", "nmDaysAllegStaff", "dtAnalystResolDt", "nmDaysAllegOut", "nmDaystoResolve", "nmDistAnaRelDays", "kwSource", "txSourceExplain", "kwAllegation", "rtxAllegation", "RtxScope", "nmRecAmount", "nmBoardWaiver", "nmAdjustments", "nmFinalRecAmount", "txCo1", "txPerson1", "txPMATitle", "txCity1",
    "txState1", "datepicker", "txMultiPNs", "txAllegComments", "approveFlag2", "OldForm", "DeletedBy", "DeletedOn", "workflowinfo", "$REF", "dtPayDue_2", "txInvoiceNo_2", "dtRecovInv_2", "dtBalPaid_2", "txtLocalCurTypeRate_2", "nmInvoiceAmtDollars_2", "nmDaysOS_2", "dtInvoice_2", "dtPayDue_3", "txInvoiceNo_3", "dtRecovInv_3", "dtBalPaid_3",
    "txtLocalCurTypeRate_3", "nmInvoiceAmtDollars_3", "nmDaysOS_3", "dtInvoice_3", "txEUserName", "txEUserCity", "txEUserState", "txEUserCountry", "txEUserName1", "txEUserCity1", "txEUserState1", "txEUserCountry1", "txEUserName2", "txEUserCity2", "txEUserState2", "txEUserCountry2", "txEUserName3", "txEUserCity3", "txEUserState3", "txEUserCountry3",
    "txEUserName4", "txEUserCity4", "txEUserState4", "txEUserCountry4", "txEUserName5", "txEUserCity5", "txEUserState5", "txEUserCountry5", "txEUserName6", "txEUserCity6", "txEUserState6", "txEUserCountry6", "txEUserName7", "txEUserCity7", "txEUserState7", "txEUserCountry7", "txEUserName8", "txEUserCity8", "txEUserState8", "txEUserCountry8",
    "txEUserName9", "txEUserCity9", "txEUserState9", "txEUserCountry9", "dtoriacrb", "txCo1_1", "txPerson1_1", "txPMATitle_1", "txCity1_1", "txState1_1", "txtInvoiceComments_2", "txtInvoiceComments_3", "dtMCRespRec", "principalname"];
*/
// COMPI 
/* var modelfields = ["Form", "nmauthor", "kwchannel", "nmmodified", "dtcreated", 
      "dtmodified", "createdby", "modifiedby", "contact", "nmphone", "SpecialConsYesNo", "BUS_LEGAL_",
      "DBA_NAME", "AKA__BUS", "AKA2_BUS", "ADDRESS", "City", "ST_ZIP", , "zip", "txtGeo", "isocountry",
      "Country_Abbr", "CountryISOCode", "PHONE", "Name", "TITLE", "nmprincipals",
      "NAME_1", "NAME_2", "NAME_3", "NAME_4", "NAME_5", "NAME_6", "NAME_7", "NAME_8",
      "NAME_9", "nmLogNo", "nmCEI", "nmInvestigationID", "nmSamRvwd", "nmSamInfrac",
      "perSamInfrac", "nmSamNDA", "perSamNDA", "nmSamTier", "nmSamDoc", "nmSamEUVal",
      "nmFinalRecvAmt", "nmBPInvestigation", "nmGrossRecovery", "nmMAMAmounts",
      "nmBoardWaivers", "SC_auditnumber", "SC_Samples", "SC_auditDate",
      "SC_CoISamples", "SC_supplierID", "SC_SupplierFinalRec", "workflowinfo", "severityLevel",
      "BRETscope", "rbExcTCsearch", "txtJustTCExclusion", "Notes", "REFERENCE", "hardcopyyesno",
     "OldForm", "DeletedBy", "DeletedOn"]; */

// mbp
/*var modelfields = ["Form","txtCurprimname","txKeywdLabel","txDBAName","txLegalName","txAddress1","txCity","txState","txZip",
"kwCountryCode","txWebsite","txCountry","txGrowthMarket","kwPartnerRel","txEntNum","txLOCID","txTelNum","txCustNum","txFaxNum",
"txMajorMarket","txGrowthMarketReg","txGeoID","txExecName","txExecTitle","txExecPhone","txExecEmail","txContName1","txContTel1",
"txContEmail1","txContTtl1","txContFax1","kwAuthProds","kwDistributor","txDistAddr1","txDistCity","txDistState","txDistZip",
"workflowinfo","kwChannel","OldForm","DeletedBy","DeletedOn","txGeo","docid"];
*/
// cvi
var modelfields = ["Form", "ActionTaken", "ApproverFlag", "nextApprover", "MoreDetailsFlag", "MailToSubmitter", "RejecterFlag", "AssignActionFlag", "InquiryID", "DocStatus", "CreatedOn", "CreatedBy",
    "Inqpurpose", "CompdeskFuncID", "BPPhone", "BpcLogno", "EmailCC", "EmailBCC", "CompISearchResult", "AsscompName", "BPLegalName", "BusName", "CEIID", "BPAddr1", "BPCity", "BPState", "BPZip", "BPCountry", "CPURL",
    "CPName", "CPLocalName", "CPCountry", "CPEmpAgrkeyrole", "CPEmpName", "CPEmpEmail", "relInqArea", "RelCompIRecords", "RelEmails", "nmauthor", "nmmodified", "dtcreated", "dtmodified", "contact", "nmphone",
    "kwchannel", "SpecialConsYesNo", "DBA_NAME", "bus_legal_", "AKA__BUS", "AKA2_BUS", "ADDRESS", "CITY", "ST_ZIP", "zip", "isocountry", "txtGeo", "CountryISOCode", "Country_Abbr", "NAME", "PHONE", "nmprincipals",
    "TITLE", "NAME_2", "NAME_1", "NAME_3", "NAME_4", "NAME_5", "NAME_6", "NAME_7", "NAME_8", "NAME_9", "nmLogNo", "nmCEI", "nmSamRvwd", "nmInvestigationID", "perSamInfrac", "nmSamInfrac", "perSamNDA", "nmSamNDA",
    "nmSamTier", "nmSamEU", "nmSamDoc", "nmSamEUVal", "nmFinalRecvAmt", "nmBPInvestigation", "nmGrossRecovery", "nmMAMAmounts", "nmBoardWaivers", "BRETscope", "severityLevel", "rbExcTCsearch", "txtJustTCExclusion",
    "NOTES ", "REFERENCE", "hardcopyyesno", "Appr1Name", "Appr1Comments", "Appr1SignDate", "Appr1Sign", "Appr2Name", "Appr2Comments", "Appr2SignDate", "Appr2Sign", "FinalApprName", "FinalApprComments",
    "FinalApprSignDate", "FinalApprSign", "Action", "userLoggedIn", "ActionOwner", "ActionDuedt", "DtReceived", "DtForwarded", "DtClosed", "TurnAround", "ActionComments", "BPCRecommend", "CompiReason",
    "AdminComments", "History", "OldForm", "DeletedBy", "DeletedOn", "PrincNameCompI", "principalname", "cviadmin", "compfunctId", "Appurl", "userdetail", "Submittercomments"];

var bpcdocs = [];
var updateddocs = [];
var lotusid = [];
var intranetid = [];
var bpcdoc = {};
var limit = 200;

// get the port to use as an HTTP server
var port = process.env.PORT || 3030
process.env.TZ = 'Asia/New_Delhi';

// set up and run the HTTP server
var app = express()

app.get('/', function (request, response) {
    response.send('This server runs Migartion cron job for BPCOPS')
})

console.log('Running HTTP server on port ' + port)
app.listen(port)

console.log('Migartion Cron job for BPCOPS!')


//Use the VCAP_SERVICES environment variable for Bluemix environments
//Use the vcap-local.json file for local environment
var vcapLocal;
try {
    vcapLocal = require('./vcap-local.json');
    console.log("Loaded local VCAP", vcapLocal);
} catch (e) {
    console.log("The vcap-local.json could not be loaded")
}

const appEnvOpts = vcapLocal ? { vcap: vcapLocal } : {}
const appEnv = cfenv.getAppEnv(appEnvOpts);

//Obtain Cloudant database connection VCAP credentials
if (appEnv.services['cloudantNoSQLDB']) {
    // Load the Cloudant library.

    // Initialize database with credentials
    var cloudant = Cloudant(appEnv.services['cloudantNoSQLDB'][0].credentials);

    var dbName = appEnv.services['cloudantNoSQLDB'][0].name;
    console.log("dbName = " + dbName);

    // Specify the database we are going to use (mydb)...
    bpc_cloudant = cloudant.db.use(dbName);
    console.log("Cloudant is connected")
}

//process.env.APP_ENV is undefined for local environment
console.log("Current environment = " + process.env.APP_ENV);

//console.log(process.env)    // returns the current environment variables

//Obtain DB2 connection VCAP credentials
if (appEnv.services['dashDB For Transactions']) {
    var db2_connection = appEnv.services['dashDB For Transactions'][0].credentials.dsn;
}

ibmdb.open(db2_connection, function (err, conn) {
    if (err) {
        console.error("DB2 connection error: ", err.message);
    } else {
        console.log("DB2 connection available");
    }
})

//set up cron jobs
new CronJob('30 19 * * *', everyHour(), null, true)

//bpcops review form
var mailidfunction = function () {
    var mail1 = [];
    lotusid = [];
    intranetid = [];
    //reviewform
    // let intranetquery = { selector: { Form: { "$eq": "fFXKeyword" }, txFXKeyword: "IntranetIds" } };

    //cvi
    // let intranetquery = { selector: { Form: { "$eq": "fKeywordCVI" }, KeywordName: "IntranetIdCVIs" } }

    bpc_cloudant.find(intranetquery, function (er, mailid) {
        if (er) {
            console.log(er);
            return;
        }

        //compi
        let intranetquery = { selector: { Form: { "$eq": "fKeywordCVI" }, KeywordName: "IntranetIds" } };

        bpc_cloudant.find(intranetquery, function (er, mailid) {
            if (er) {
                console.log(er);
                return reject(er);
            }

            //reviewform
            /*      mailidlist = (mailid.docs[0].txFXKeyList).split(";");
                  for (var i = 0; i < mailidlist.length; i++) {
                      mail1 = mailidlist[i].split("-");
                      lotusid.push(mail1[0]);
                      intranetid.push(mail1[1]);
                  }
          */
            // cvi & compi
            mailidlist = (mailid.docs[0].KeywordList).split(";");

            for (var i = 0; i < mailidlist.length; i++) {
                mail1 = mailidlist[i].split("::");
                lotusid.push(mail1[0]);
                intranetid.push(mail1[1]);
            }
            console.log(lotusid.length);
            console.log(intranetid.length);
        })
        return (true);
    });
}

function updatedoc() {
    var keys1 = [];

    // REVIEWFORM
    //let query = { selector: { Form: { "$in": ["fInfo", "fInfoAP", "fInfoEMEA"] }, txLogNo: "AM-05085" } };
    //   let query = { selector: { Form: { "$in": ["fInfo", "fInfoAP", "fInfoEMEA"] } , Archive: { "$exists": false } }};
    //  let query = { selector: { Form: { "$in": ["fInfo", "fInfoAP", "fInfoEMEA"] }, txLogNo: "AM-06089" } };
    //compi 
    // let query = {selector: { Form: { "$eq": "frecord" }}}

    //MBP
    // let query = {selector: { Form: { "$eq": "fMainBusPrtnr" }}}

    //cvi
    let query = { selector: { Form: { "$eq": "frmInquiry" } } }

    // reviewform 
    mailidfunction();

    console.log("query", JSON.stringify(query));
    bpc_cloudant.find(query, function (err, result) {
        if (err) {
            console.log(err);
            return;
        }
        bpcdocs = result.docs;
        console.log("Length", bpcdocs.length);

        //CASE sensitive check
        /* _.forEach(modelfields, function (value, key) {
             keys1.push(_.toUpper(value));
         }) */
        _.forEach(bpcdocs, (doc, docindex) => {
            doc = bpcdocs[docindex];
            /*         _.forEach(doc, (value, key) => {
                         if (key === "nmAddEditor") {
                             if(!(doc[key] instanceof Array))
                             var maillist = _.split(doc[key], ";");
                             else
                             var maillist = doc[key];
                             _.forEach(maillist, (value, key) => {
                                 value = value.replace(/CN=+|CN=$/gm, '');
                                 value = value.replace(/OU=+|OU=$/gm, '');
                                 value = value.replace(/OU=+|OU=$/gm, '');
                                 maillist[key] = value.replace(/O=+|O=$/gm, '');
                                 var intranetindex = _.indexOf(lotusid, maillist[key]);
                                 if (_.indexOf(lotusid, maillist[key]) != -1) {
                                     maillist[key] = intranetid[intranetindex];
                                 }
                             })
                             doc[key] = maillist;
                         }
                     }); */
            // for (var i = 0; i < bpcdocs.length; i++) {
            /* _.forEach(bpcdocs, (doc, docindex) => {
                 var i = 0;
                 doc = bpcdocs[docindex];
                 _.forEach(doc, (value, key) => {
                     var key1 = _.toUpper(key);
                     var index1 = _.indexOf(keys1, _.toUpper(key));
                     if (index1 != -1 && modelfields[index1] !== key) {
                         bpcdocs[docindex][modelfields[index1]] = value;
                         delete bpcdocs[docindex][key];
                     }
                 });
     */
            //Notes Email id conversion Review form
            /*      _.forEach(doc, (value, key) => {
                      if ((key === "txStaffMermberEmail") || (key === "kwStaffMember") || (key === "nmAddEditor") || (key === "kwStaffMember_1") || (key === "signature")) {
                          value = value.replace(/CN=+|CN=$/gm, '');
                          value = value.replace(/OU=+|OU=$/gm, '');
                          doc[key] = value.replace(/O=+|O=$/gm, '');
                          if (doc[key].includes(";")) {
                              var maillist = doc[key].split(";");
                              _.forEach(maillist, (value, key) => {
                                  var intranetindex = _.indexOf(lotusid, value);
                                  if (_.indexOf(lotusid, value) != -1) {
                                      maillist[key] = intranetid[intranetindex];
                                  }
                              })
                              doc[key] = maillist.join(";");
                          } else {
                              var intranetindex = _.indexOf(lotusid, doc[key]);
                              if (_.indexOf(lotusid, doc[key]) != -1) {
                                  doc[key] = intranetid[intranetindex];
                              }
                          }
                      }
                  });
      */
            //Notes Email id conversion COMPI
            _.forEach(doc, (value, key) => {
                //  value = value.replace(/CN=+|CN=$/gm,'');
                // value=value.replace(/OU=+|OU=$/gm,'');
                // value=value.replace(/O=+|O=$/gm,'');
                if ((key === "createdby") || (key === "modifiedby") || (key === "contact")) {
                    var intranetindex = _.indexOf(lotusid, value);
                    // console.log(intranetindex);
                    if (_.indexOf(lotusid, value) != -1) {
                        doc[key] = intranetid[intranetindex];
                    }
                }
            });

            //Notes Email id conversion CVI
            /*    _.forEach(doc, (value, key) => {
                    if ((key === "nextApprover") || (key === "CreatedBy") || (key === "Appr1Name") || (key === "Appr2Name") || (key === "FinalApprName") || (key === "ActionOwner") || (key === "contact")) {
                        var intranetindex = _.indexOf(lotusid, doc[key]);
                        console.log(intranetindex);
                        if (_.indexOf(lotusid, value) != -1) {
                            doc[key] = intranetid[intranetindex];
                        }
                    }
                }); 
  
              //Field mapping
              /*          if ((doc.nmRecAmount == "null") && (kwInvestType == "Allegation" || kwInvestType == "Management Call")) {
                            doc.nmRecAmount = doc.nmTotDollarAmt;
                        }
            
                        if ((doc.nmFinalRecAmount == "null") && (kwInvestType == "Allegation" || kwInvestType == "Management Call")) {
                            doc.nmFinalRecAmount = doc.nmFinalRecvAmt;
                        }
            
                        if (doc.TSDNumSamples == "null" || doc.TSDNumSamples == 0) {
                            doc.TSDNumSamples = doc.nmNoFSrNo;
                        }
            
                        if (doc.TSDNumSamplesNDADec == "null" || doc.TSDNumSamplesNDADec == 0) {
                            doc.TSDNumSamplesNDADec = doc.nmBidsNDA;
                        }
            
                        if (doc.TSDNetRecovery == "") {
                            if (doc.nmFinalRecvAmt != "") {
                                doc.TSDNetRecovery = doc.nmFinalRecvAmt;
                            }
                            else if (doc.nmFinalRecvAmtLA != "") {
                                doc.TSDNetRecovery = doc.nmFinalRecvAmtLA;
                            }
                        }
            
                      /*  if (doc.Recovery_Amount_text != "" + doc.txRecSummary && (doc.Recovery_Amount_text == "")) {
                            doc.Recovery_Amount_text = doc.Recovery_Amount_text + "\nRec Summary:" + doc.txRecSummary;
                        }*/

            /*         if (!(doc.Recovery_Amount_text.includes("Rec Summary:")) && (doc.Recovery_Amount_text == "")) {
                         doc.Recovery_Amount_text = "Rec Summary:" + doc.txRecSummary;
                     }
                     else {
                         doc.Recovery_Amount_text = doc.Recovery_Amount_text + "\nRec Summary:" + doc.txRecSummary;
                     } 
        
                    if (doc.nmTotDollarAmt == "null" || doc.nmTotDollarAmt == 0)
                        doc.nmTotDollarAmt = doc.TSDNetRecovery
        
                    if (doc.nmFinalRecvAmt == "null" || doc.nmFinalRecvAmt == 0)
                        doc.nmFinalRecvAmt = doc.TSDNetRecovery
        
                 /*   if (doc.txGeo == "EMEA") {
        
                            doc.dtPayDue_3 = doc.dtPayDue_2;
                            doc.dtRecovInv_3 = doc.dtRecovInv_2;
                            doc.txInvoiceNo_3 = doc.txInvoiceNo_2;
                            doc.dtBalPaid_3 = doc.dtBalPaid_2;
                            doc.txtLocalCurTypeRate_3 = doc.txtLocalCurTypeRate_2;
                            doc.dtInvoice_3 = doc.dtInvoice_2;
                            doc.nmInvoiceAmtDollars_3 = doc.nmInvoiceAmtDollars_2;
                            doc.nmDaysOS_3 = doc.nmDaysOS_2;
                            doc.txtInvoiceComments_3 = doc.txtInvoiceComments_2;
        
        
        
                            doc.dtPayDue_2 = doc.dtPayDue_1;
                            doc.dtRecovInv_2 = doc.dtRecovInv_1;
                            doc.txInvoiceNo_2 = doc.txInvoiceNo_1;
                            doc.dtBalPaid_2 = doc.dtBalPaid_1;
                            doc.txtLocalCurTypeRate_2 = doc.txtLocalCurTypeRate_1;
                            doc.dtInvoice_2 = doc.dtInvoice_1;
                            doc.nmInvoiceAmtDollars_2 = doc.nmInvoiceAmtDollars_1;
                            doc.nmDaysOS_2 = doc.nmDaysOS_1;
                            doc.txtInvoiceComments_2 = doc.txtInvoiceComments_1;
        
        
                            doc.dtPayDue_1 = doc.dtPayDue;
                            doc.dtRecovInv_1 = doc.dtRecovInv;
                            doc.txInvoiceNo_1 = doc.txInvoiceNo;
                            doc.dtBalPaid_1 = doc.dtBalPaid;
                            doc.txtLocalCurTypeRate_1 = doc.txtLocalCurTypeRate;
                            doc.dtInvoice_1 = doc.dtInvoice;
                            doc.nmInvoiceAmtDollars_1 = doc.nmInvoiceAmtDollars;
                            doc.nmDaysOS_1 = doc.nmDaysOS;
                            doc.txtInvoiceComments_1 = doc.txtInvoiceComments;
        
                        }*/

            /*          if (doc.txGeo == "AP") {
                          doc.dtPayDue_1 = doc.dtPayDue;
                          doc.dtRecovInv_1 = doc.dtRecovInv;
                          doc.txInvoiceNo_1 = doc.txInvoiceNo;
                          doc.dtBalPaid_1 = doc.dtBalPaid;
                          doc.txtLocalCurTypeRate_1 = doc.txtLocalCurTypeRate;
                          doc.dtInvoice_1 = doc.dtInvoice;
                          doc.nmInvoiceAmtDollars_1 = doc.nmInvoiceAmtDollars;
                          doc.nmDaysOS_1 = doc.nmDaysOS;
                          doc.txtInvoiceComments_1 = doc.txtInvoiceComments;
                      }
      */
            updateddocs.push(doc);
        });
        console.log(new Date());
        recursiveBulk();
    });
}


function recursiveBulk() {
    if (!bpcdocs || bpcdocs.length === 0) {
        return (new Error('No docs provided'));
    }
    console.log("Items left to process", bpcdocs.length);
    // remove first up to limit docs
    let cloudantData = bpcdocs.splice(0, limit);
    console.log("Cloudant updates for: " + cloudantData.length + " records");
    bpc_cloudant.bulk({ docs: cloudantData }, function (err, result) {
        if (err) {
            console.log("Cloudant Insert bulk err", err);
            throw (err);
        }
        if (bpcdocs.length > 0) {
            // do next batch
            recursiveBulk();
        } else {
            console.log("migartion cron is succesfully completed");
            return true;
        }
    });
}

function everyHour() {
    return function () {
        ibmdb.open(db2_connection, function (err, conn) {
            if (err) {
                console.error('error: ', err.message);
            } else {
                console.log(new Date());
                console.log('Connected to the BLUDB');
                updatedoc();
                console.log(new Date());
            }
        });
    }
}
